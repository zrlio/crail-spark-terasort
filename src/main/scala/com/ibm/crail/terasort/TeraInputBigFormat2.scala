/**
  * Created by atr on 04.11.16.
  */
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

package com.ibm.crail.terasort

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.List

import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.spark.TaskContext

import scala.collection.JavaConversions._


class TeraInputBigFormat2 extends FileInputFormat[Array[Byte], Array[Byte]] {

  override final def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[Array[Byte], Array[Byte]] = new TeraRecordReader()

  override final def listStatus(job: JobContext): List[FileStatus] = {
    val listing = super.listStatus(job)
    val sortedListing= listing.sortWith{ (lhs, rhs) => {
      lhs.getPath().compareTo(rhs.getPath()) < 0
    } }
    sortedListing.toList
  }

  class TeraRecordReader extends RecordReader[Array[Byte], Array[Byte]] {

    private var in : FSDataInputStream = null
    private var inputBuffer: Array[Byte] = null
    private var inputSerBuffer: SerializerBuffer = null
    private var totalIncomingBytes:Long = 0
    private var processedSoFar:Long= 0
    private val inputBufferSize = TaskContext.get().getLocalProperty(TeraConf.inputBufferSizeKey).toInt
    private var readFromHdfs = 0
    private val verbose = TaskContext.get().getLocalProperty(TeraConf.verboseKey).toBoolean

    private val bigKeyBuffer = ByteBuffer.allocate(Integer.BYTES) // the size of Int on the machine

    /* accompaning spark-io code */
//    def writeBig(records: Iterator[Product2[K, V]]): Unit = {
//      val iter = if (dep.aggregator.isDefined) {
//        if (dep.mapSideCombine) {
//          dep.aggregator.get.combineValuesByKey(records, context)
//        } else {
//          records
//        }
//      } else {
//        require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
//        records
//      }
//      /* now we know that first key is the size and second is the number of elements */
//      while(records.hasNext){
//        val record = records.next()
//        val x = ByteBuffer.wrap(record._1.asInstanceOf[Array[Byte]]).getInt
//        require(x % 100 == 0, " x is no good " + x + " <- ")
//        val entries = x / 100
//        val buffer = record._2.asInstanceOf[Array[Byte]]
//        var index = 0
//        System.err.println(" TID: " + TaskContext.get.taskAttemptId() + " processing in bulk with " + entries + " keys ")
//        for (i <- 0 until entries){
//          /* loop for i then now we write out */
//          val key = buffer.slice(index, index + 10)
//          index+=10
//          val value = buffer.slice(index, index + 90)
//          index+=90
//          val bucketId = dep.partitioner.getPartition(key)
//          shuffle.writers(bucketId).write(key, value)
//        }
//      }
//    }
    private def refillInputBuffer(): Int = {
      val start = System.nanoTime()
      var read : Int = 0
      var newRead : Int = -1
      val target = Math.min(totalIncomingBytes - processedSoFar, inputBufferSize).toInt

      if(verbose) {
        System.err.println(TeraConf.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " attempting to refill the buffer with " + target + " bytes")
      }
      while (read < target) {
        newRead = in.read(inputBuffer, read, target - read)
        if(newRead == -1) {
          throw new EOFException("read past eof")
        }
        // otherwise we add to what we have read
        read += newRead
      }

      readFromHdfs+=1
      /* update the size */
      bigKeyBuffer.clear()
      bigKeyBuffer.putInt(target) //the current size that we have copied

      if(verbose) {
        val timeUs = (System.nanoTime() - start) / 1000
        System.err.println(TeraConf.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " HDFS read bytes: " + target +
          " time : " + timeUs + " usec , or " +
          (target.asInstanceOf[Long] * 8) / timeUs + " Mbps ")
      }
      target
    }

    override final def nextKeyValue() : Boolean = {
      //don't use Array[Byte].length for any calculation as we might have a buffer that is bigger than what we asked
      if (processedSoFar >= totalIncomingBytes) {
        return false
      }
      /* prepare next read */
      val newStuff = refillInputBuffer()
      processedSoFar+= newStuff
      true
    }

    override final def initialize(split : InputSplit, context : TaskAttemptContext) = {
      val reclen = TeraConf.INPUT_RECORD_LEN
      val fileSplit = split.asInstanceOf[FileSplit]
      val p : Path = fileSplit.getPath
      val fs : FileSystem = p.getFileSystem(context.getConfiguration)
      fs.setVerifyChecksum(false)
      //val fileSize = fs.getFileStatus(p).getLen
      in = fs.open(p)
      // find the offset to start at a record boundary
      val offset: Long = (reclen - (fileSplit.getStart % reclen)) % reclen
      val start = offset + fileSplit.getStart
      in.seek(start)
      // check how much is available in the file, with a full multiple this should be good
      val length: Long = fileSplit.getLength - offset
      val rem = (start + length)%TeraConf.INPUT_RECORD_LEN
      val endOffset = if(rem == 0) start + length else (start + length + (TeraConf.INPUT_RECORD_LEN - rem))
      totalIncomingBytes = (endOffset - start)

      require((totalIncomingBytes % TeraConf.INPUT_RECORD_LEN) == 0 ,
        " incomingBytes did not alight : " + totalIncomingBytes + " mod : " + (totalIncomingBytes % TeraConf.INPUT_RECORD_LEN))

      inputSerBuffer = BufferCache.getInstance().getByteArrayBuffer(inputBufferSize)
      inputBuffer = inputSerBuffer.getByteArray
      if(verbose){
        System.err.println(TeraConf.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " starting, will process " + totalIncomingBytes + " bytes wih buffer size: " + inputBufferSize)
      }
    }


    override final def close() = {
      BufferCache.getInstance().putBuffer(inputSerBuffer)
      in.close()
      if(verbose){
        System.err.println(TeraConf.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " finished, processed " + processedSoFar + " bytes, with " +readFromHdfs + " times to HDFS, "
          + BufferCache.getInstance.getCacheStatus)
      }
    }
    override final def getCurrentKey : Array[Byte] = bigKeyBuffer.array()
    override final def getCurrentValue : Array[Byte] = inputBuffer
    override final def getProgress : Float = processedSoFar / totalIncomingBytes
  }
}