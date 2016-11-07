/*
 * Crail-terasort: An example terasort program for Sprak and crail
 *
 * Author: Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.terasort.sorter

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import com.ibm.crail.terasort.{BufferCache, TeraConf}
import com.ibm.radixsort.NativeRadixSort
import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.crail.{CrailDeserializationStream, CrailShuffleSorter}
import sun.nio.ch.DirectBuffer

import scala.collection.mutable.ListBuffer

private case class OrderedByteBuffer(buf: ByteBuffer) extends Ordered[OrderedByteBuffer] {
  override def compare(that: OrderedByteBuffer): Int = {
    /* read int from the current position */
    val thisInt = this.buf.getInt
    /* revert */
    buf.position(this.buf.position() - Integer.BYTES)
    /* read int from the current position */
    val thatInt = that.buf.getInt
    /* revert */
    that.buf.position(that.buf.position() - Integer.BYTES)
    /* compare and return results */
    thisInt - thatInt
  }

  /* just for debugging */
  override def toString : String = {
    buf.toString
  }
}

private class OrderedByteBufferCache(size: Int) {

  private val cacheBufferList = ListBuffer[OrderedByteBuffer]()
  private val get = new AtomicLong(0)
  private val put = new AtomicLong(0)
  private val miss = new AtomicLong(0)

  def getBuffer : OrderedByteBuffer = {
    get.incrementAndGet()
    this.synchronized {
      if (cacheBufferList.nonEmpty) {
        val ret = cacheBufferList.head
        cacheBufferList.trimStart(1)
        return ret
      }
    }
    miss.incrementAndGet()
    /* otherwise we are here then ...allocate new */
    OrderedByteBuffer(ByteBuffer.allocateDirect(size))
  }

  def putBuffer(buf: OrderedByteBuffer) : Unit = {
    put.incrementAndGet()
    this.synchronized {
      buf.buf.clear()
      cacheBufferList += buf
    }
  }

  def getStatistics:String = {
    TeraConf.verbosePrefixCache + " TID: " + TaskContext.get().taskAttemptId() + " OrderedCache: totalAccesses " +
      get.get() + " misses " + miss.get() + " puts " + put.get() +
      " hitrate " + ((get.get() - miss.get) * 100 / get.get()) + " %"
  }
}

private object OrderedByteBufferCache {
  private var instance:OrderedByteBufferCache = null

  final def getInstance():OrderedByteBufferCache = {
    this.synchronized {
      if(instance == null) {
        val size = TaskContext.get().getLocalProperty(TeraConf.f22BufSizeKey).toInt
        instance = new OrderedByteBufferCache(size)
      }
    }
    instance
  }
}

class CrailShuffleNativeRadixSorter extends CrailShuffleSorter {
  override def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser: Serializer,
                          inputStream: CrailDeserializationStream): Iterator[Product2[K, C]] = {

    val verbose = TaskContext.get().getLocalProperty(TeraConf.verboseKey).toBoolean
    /* we collect data in a list of OrderedByteBuffer */
    val bufferList = ListBuffer[OrderedByteBuffer]()
    var totalBytesRead = 0
    val expectedRead = TaskContext.get().getLocalProperty(TeraConf.f22BufSizeKey).toInt
    val useBigIterator = TaskContext.get().getLocalProperty(TeraConf.useBigIteratorKey).toBoolean
    var bytesRead = expectedRead // to start the loop
    while(bytesRead == expectedRead) {
      /* this needs to be a multiple of KV size otherwise we will break the record boundary */
      val oBuf = OrderedByteBufferCache.getInstance().getBuffer
      bytesRead = inputStream.read(oBuf.buf)
      require(bytesRead % TeraConf.INPUT_RECORD_LEN == 0,
        " bytesRead " + bytesRead + " is not a multiple of the record length " + TeraConf.INPUT_RECORD_LEN)
      /* from F22 semantics, when we hit EOF we will get 0 */
      if(bytesRead >= 0) {
        /* if we did not read -1, which is EOF then insert - make sure to flip ;) */
        oBuf.buf.flip()
        bufferList+=oBuf
        /* once we have it then lets sort it */
        NativeRadixSort.sort(oBuf.buf.asInstanceOf[DirectBuffer].address() /* address */,
          bytesRead/TeraConf.INPUT_RECORD_LEN /* number of elements */,
          TeraConf.INPUT_KEY_LEN /* can use on the serializer interface of keySize and valueSize */,
          TeraConf.INPUT_RECORD_LEN)
      }
      totalBytesRead+=bytesRead
      /* now if we have read less than expected, that would be the end of the file */
    }
    if(verbose) {
      System.err.println(TeraConf.verbosePrefixSorter + " TID: " + TaskContext.get().taskAttemptId() +
        " assembled " + totalBytesRead + " bytes in " + bufferList.length + " buffers")
    }

    require(totalBytesRead % TeraConf.INPUT_RECORD_LEN == 0 ,
      " totalBytesRead " + totalBytesRead + " is not a multiple of the record length " + TeraConf.INPUT_RECORD_LEN)
    if(useBigIterator) {
      new ByteBufferBigIterator(bufferList, totalBytesRead, verbose).asInstanceOf[Iterator[Product2[K, C]]]
    } else {
      new ByteBufferIterator(bufferList, totalBytesRead, verbose).asInstanceOf[Iterator[Product2[K, C]]]
    }
  }
}

private class ByteBufferIterator(bufferList: ListBuffer[OrderedByteBuffer], totalBytesRead: Int, verbose: Boolean)
  extends Iterator[Product2[Array[Byte], Array[Byte]]] {

  private val key = new Array[Byte](TeraConf.INPUT_KEY_LEN)
  private val value = new Array[Byte](TeraConf.INPUT_VALUE_LEN)
  private val numElements = totalBytesRead/TeraConf.INPUT_RECORD_LEN
  private var processed = 0
  private val kv = (key, value)
  private var currentMin = bufferList.head
  private var bufferReturned = false

  override def hasNext: Boolean = {
    val more = processed < numElements
    if(!more && !bufferReturned){
      val ins = OrderedByteBufferCache.getInstance()
      /* hit the end */
      bufferList.foreach(p => ins.putBuffer(p))
      bufferReturned = true
      if(verbose) {
        System.err.println(ins.getStatistics)
      }
    }
    more
  }

  def calcNextMinBuffer(): Unit = {
    // we read from buffer then that means we have to reset it too
    bufferList.foreach(p => {
      if(p.buf.remaining() > 0 && (currentMin > p))
        currentMin = p
    })
  }

  override def next(): Product2[Array[Byte], Array[Byte]] = {
    /* now we need to walk over the list to find out where the min is */
    calcNextMinBuffer()
    /* now we have the min, we copy it out */
    processed += 1
    currentMin.buf.get(key)
    currentMin.buf.get(value)
    if(currentMin.buf.remaining() == 0) {
      /* we hit the end of the current buffer, time to refresh */
      val itr = bufferList.iterator
      val oldCurrentMin = currentMin
      /* find any buffer which has non-zero capacity */
      while (itr.hasNext && currentMin == oldCurrentMin){
        val x = itr.next()
        if(x.buf.remaining() > 0) {
          /* setting this will break the loop as well */
          currentMin = x
        }
      }
    }
    kv
  }
}

private class ByteBufferBigIterator(bufferList: ListBuffer[OrderedByteBuffer], totalBytesRead: Int, verbose: Boolean)
  extends Iterator[Product2[Array[Byte], Array[Byte]]] {

  require(bufferList.length == 1, " Jonas: BigIterator only works with single buffer, currently " +
    bufferList.length + " buffers, with total data size of " + totalBytesRead)

  require(bufferList.head.buf.remaining() == totalBytesRead,
    " remaining is : " + bufferList.head.buf.remaining() + " total Bytes : " + totalBytesRead)

  private var processed = 0
  private val bufferSize = TaskContext.get().getLocalProperty(TeraConf.outputBufferSizeKey).toInt
  private val bigSerBuffer = BufferCache.getInstance().getByteArrayBuffer(bufferSize)
  private val bigValue = bigSerBuffer.getByteArray
  private val bigKeyBuffer = ByteBuffer.allocate(Integer.BYTES) // the size of Int on the machine
  /* the byte array that we will return */
  private var bigKey = bigKeyBuffer.array()
  /* if we have hit the end and time to return the buffer */
  private var done = false
  private var bufferReturned = false

  if(verbose) {
    System.err.println(TeraConf.verbosePrefixIterator + " TID: " + TaskContext.get().taskAttemptId() +
      " BigIterator initialized with toalBytes: " + totalBytesRead + " bufferSize " + bufferSize)
  }

  def refillBuffer(): Unit = {
    /* now we extract the min */
    val min = Math.min(bufferSize, bufferList.head.buf.remaining())
    /* we copy these many bytes into the byte buffer */
    bufferList.head.buf.get(bigValue, 0, min)
    /* we set the key size, first reset and then put */
    bigKeyBuffer.clear()
    bigKeyBuffer.putInt(min) //the current size that we have copied
    bigKey = bigKeyBuffer.array()
    processed+=min // update the local counter
  }

  override def hasNext: Boolean = {
    /* if we have remaining then we are not done */
    if(done && !bufferReturned) {
      /* if we are done then return the buffer */
      val ins = OrderedByteBufferCache.getInstance()
      ins.putBuffer(bufferList.head)
      BufferCache.getInstance().putBuffer(bigSerBuffer)
      if (verbose) {
        System.err.println(ins.getStatistics + " putting the buffer down. ")
        System.err.println(TeraConf.verbosePrefixIterator + " TID: " + TaskContext.get().taskAttemptId() +
          BufferCache.getInstance().getCacheStatus + " putting the buffer down. ")
      }
      bufferReturned = true
    }
    !done
  }

  override def next(): Product2[Array[Byte], Array[Byte]] = {
    /* every time we refill the buffer */
    refillBuffer()
    if(processed == totalBytesRead) {
      done = true
    }
    (bigKey, bigValue)
  }
}