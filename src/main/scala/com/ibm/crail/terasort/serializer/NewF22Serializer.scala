/*
 * Crail-terasort: An example terasort program for Sprak and crail
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *         Jonas Pfefferle <jpf@zurich.ibm.com>
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

package com.ibm.crail.terasort.serializer

/**
  * Created by atr on 09.11.16.
  */

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.Future

import com.ibm.crail.terasort.TeraConf
import com.ibm.crail._
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.shuffle.crail.{CrailDeserializationStream, CrailSerializationStream, CrailSerializerInstance, CrailShuffleSerializer}
import org.apache.spark.{ShuffleDependency, TaskContext}

import scala.reflect.ClassTag

class NewF22Serializer() extends Serializer with Serializable with CrailShuffleSerializer {
  override final def newInstance(): SerializerInstance = {
    F22ShuffleSerializerInstance.getInstance()
  }
  override lazy val supportsRelocationOfSerializedObjects: Boolean = true

  override def newCrailSerializer[K,V](dep: ShuffleDependency[K,_,V]): CrailSerializerInstance = {
    NewF22ShuffleSerializerInstance.getInstance()
  }
}

/* this class does not have a state, hence can be given back the same */
class NewF22ShuffleSerializerInstance() extends SerializerInstance with CrailSerializerInstance {

  override final def serialize[T: ClassTag](t: T): ByteBuffer = {
    throw new IOException("this call is not yet supported : serializer[] " +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  override final def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    throw new IOException("this call is not yet supported : deserialize[]" +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  override final def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    throw new IOException("this call is not yet supported : deserialize with classloader" +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  override final def serializeStream(s: OutputStream): SerializationStream = {
    throw new IOException("this call is not yet supported : serializerStream with OutputStream " +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  /* this is the one we are interested in */
  override final def deserializeStream(s: InputStream): DeserializationStream = {
    throw new IOException("this call is not yet supported : deserializerStream with InputStream " +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  override def serializeCrailStream(s: CrailBufferedOutputStream): CrailSerializationStream = {
    new F22SerializerStream(s)
  }

  override def deserializeCrailStream(s: CrailMultiStream): CrailDeserializationStream = {
    new F22DeserializerStream(s)
  }

  def newSerializeCrailStream(futureOutput: Future[CrailFile], fs: CrailFS): CrailSerializationStream = {
    new NewF22SerializerStream(futureOutput, fs)
  }

  def newDeserializeCrailStream(s: CrailMultiStream): CrailDeserializationStream = {
    new NewF22DeserializerStream(s)
  }
}

object NewF22ShuffleSerializerInstance {
  private var serIns:NewF22ShuffleSerializerInstance = null

  final def getInstance():NewF22ShuffleSerializerInstance = {
    this.synchronized {
      if(serIns == null)
        serIns = new NewF22ShuffleSerializerInstance()
    }
    serIns
  }
}

class NewF22SerializerStream(futureOutput: Future[CrailFile], fs: CrailFS) extends CrailSerializationStream {

  /* we start without a stream */
  var outputStream: CrailOutputStream = null
  /* but we get a buffer */
  val buffer0 = fs.allocateBuffer()
  val halfSize = buffer0.capacity() / 2
  /* we then split it - buffer1 is the first half, buffer2 is the second half */
  val buffer1 = buffer0.slice()
  var future1:Option[Future[CrailResult]] = None

  val buffer2 = buffer0.slice()
  var future2:Option[Future[CrailResult]] = None

  /* current pointer to the buffer */
  var currentBuffer = buffer1

  def allocateStream() : Unit = {
    if(outputStream == null){
      /* we have not open the stream yet */
      val x = futureOutput.get()
      System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " opening a new CoreFile, fd " + x.getFd)
      outputStream = x.getDirectOutputStream(Integer.MAX_VALUE)
    }
  }

  def checkIfBufferCanBeUsed(buffer: ByteBuffer) : Unit  = {
    if(buffer == buffer1){
      /* get() the value if buffer1 was in action */
      future1.foreach(fx => fx.get())
      /* we make in progress to None - we just did a get() */
      future1 = None
      /* after this we clear the buffer */
      buffer1.clear()
      /* now buffer1 is good to go */
      System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " buffer1 is good to go " + buffer1)
    } else {
      /* get() the value if buffer1 was in action */
      future2.foreach(fx => fx.get())
      /* we make in progress to None - we just did a get() */
      future2 = None
      /* after this we by hand reset the buffer */
      buffer2.position(halfSize)
      buffer2.limit(buffer0.capacity())
      /* now buffer2 is good to go */
      System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " buffer2 is good to go " + buffer2)
    }
  }

  def flushBuffer1(): Unit = {
    allocateStream()
    /* for buffer 1 flipping would be enough */
    buffer1.flip()
    val toWrite = buffer1.limit()
    if(toWrite > 0) {
      System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " flushing buffer1 with " + buffer1)
      /* this is a async operation, we save the reference */
      future1 = Some(outputStream.write(buffer1))
    }
  }

  def flushBuffer2(): Unit ={
    allocateStream()
    /* for buffer 2 we need to explicitly set position and limit */
    /* current position becomes the limit */
    buffer2.limit(buffer2.position)
    /* and the half buffer mark becomes the new position */
    buffer2.position(halfSize)

    val toWrite = buffer2.limit() - halfSize
    if(toWrite > 0) {
      System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " flushing buffer2 with " + buffer2)
      future2 = Some(outputStream.write(buffer2))
    }
  }

  def checkAndMakeSpace(size: Int): Unit = {
    if(currentBuffer == buffer1){
      if(buffer1.position() + size > halfSize) {
        /* then we have to flush */
        flushBuffer1()
        checkIfBufferCanBeUsed(buffer2)
        currentBuffer = buffer2
        System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " current buffer switched to buffer2 " + currentBuffer)
      }
    } else {
      if(buffer2.position() + size > buffer0.capacity()) {
        /* then we have to flush the second buffer */
        flushBuffer2()
        checkIfBufferCanBeUsed(buffer1)
        currentBuffer = buffer1
        System.err.println( " TID: " + TaskContext.get().taskAttemptId() + " current buffer switched to buffer1 " + currentBuffer)
      }
    }
  }

  override final def flush(): Unit = {
    /* we flush and make sure we wait for it */
    if(currentBuffer == buffer1) {
      flushBuffer1()
    } else {
      flushBuffer2()
    }
    /* it might the case the buffer1 is in flight, and we close buffer2, hence we need to wait for both */
    future1.foreach(fx => fx.get())
    future2.foreach(fx => fx.get())
  }

  override final def writeObject[T: ClassTag](t: T): SerializationStream = {
    /* explicit byte casting */
    val b = t.asInstanceOf[Array[Byte]]
    checkAndMakeSpace(b.length)
    /* we now copy it into the buffer */
    currentBuffer.put(b)
    this
  }

  override final def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)

  override final def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)

  override final def close(): Unit = {
    /* when you close you flush */
    flush()
    fs.freeBuffer(buffer0)
    if (outputStream != null) {
      outputStream.close()
    }
  }

  override final def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}

/* this will mostly stay the same */
class NewF22DeserializerStream(inStream: CrailMultiStream) extends CrailDeserializationStream {

  val verbose = TaskContext.get().getLocalProperty(TeraConf.verboseKey).toBoolean

  override final def readObject[T: ClassTag](): T = {
    throw new IOException("this call is not yet supported : readObject " +
      " \n perhaps you forgot to set spark.crail.shuffle.sorter setting in your spark conf to match F22")
  }

  override def read(buf: ByteBuffer): Int = {
    val start = System.nanoTime()
    /* we attempt to read min(in file, buf.remaining) */
    val asked = buf.remaining()
    var soFar = 0
    while ( soFar < asked) {
      val ret = inStream.read(buf)
      if(ret == -1) {
        if(verbose) {
          val timeUs = (System.nanoTime() - start) / 1000
          val bw = soFar.asInstanceOf[Long] * 8 / (timeUs + 1) //just to avoid divide by zero error
          System.err.println(TeraConf.verbosePrefixF22 + " TID: " + TaskContext.get().taskAttemptId() +
            " crail reading bytes : " + soFar + " in " + timeUs + " usec or " + bw + " Mbps")
        }
        /* we have reached the end of the file */
        return soFar
      }
      soFar+=ret
    }
    require(soFar == asked, " wrong read logic, asked: " + asked + " soFar " + soFar)
    if(verbose) {
      val timeUs = (System.nanoTime() - start) / 1000
      val bw = soFar.asInstanceOf[Long] * 8 / (timeUs + 1) //just to avoid divide by zero error
      System.err.println(TeraConf.verbosePrefixF22 + " TID: " + TaskContext.get().taskAttemptId() +
        " crail reading bytes : " + soFar + " in " + timeUs + " usec or " + bw + " Mbps")
    }
    soFar
  }

  override final def readKey[T: ClassTag](): T = {

    val key = new Array[Byte](TeraConf.INPUT_KEY_LEN)
    val ret = inStream.read(key)
    if(ret == -1) {
      /* mark the end of the stream : this is caught by spark to mark EOF - duh ! */
      throw new EOFException()
    }
    key.asInstanceOf[T]
  }

  override final def readValue[T: ClassTag](): T = {
    val value = new Array[Byte](TeraConf.INPUT_VALUE_LEN)
    val ret = inStream.read(value)
    if(ret == -1) {
      /* mark the end of the stream : this is caught by spark to mark EOF - duh ! */
      throw new EOFException()
    }
    value.asInstanceOf[T]
  }

  override final def close(): Unit = {
    if (inStream != null) {
      inStream.close()
    }
  }

  override def available(): Int = {
    //FIMXE: this is not ready, don't use this interface
    inStream.available()
  }
}
