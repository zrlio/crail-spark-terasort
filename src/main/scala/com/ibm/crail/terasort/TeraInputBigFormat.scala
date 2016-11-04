package com.ibm.crail.terasort

/**
  * Created by atr on 04.11.16.
  */
import java.io.EOFException
import java.util.List

import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.TaskContext

import scala.collection.JavaConversions._


class TeraInputBigFormat extends FileInputFormat[Array[Byte], Array[Byte]] {

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
    private var offset: Long = 0
    private var length: Long = 0
    private var key: Array[Byte] = null
    private var value: Array[Byte] = null

    private var inputBuffer: Array[Byte] = null
    private var inputSerBuffer: SerializerBuffer = null
    private var totalIncomingBytes:Long = 0
    private var processedSoFar:Long= 0
    private val inputBufferSize = TaskContext.get().getLocalProperty(TeraSort.inputBufferSizeKey).toInt
    private var inputBufferIndex = 0
    private var readFromHdfs = 0
    private val verbose = TaskContext.get().getLocalProperty(TeraSort.verboseKey).toBoolean


    private def refillInputBuffer(): Unit = {
      val start = System.nanoTime()
      var read : Int = 0
      var newRead : Int = -1
      val target = Math.min(totalIncomingBytes - processedSoFar, inputBufferSize).toInt

      if(verbose) {
        System.err.println(TeraSort.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
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
      /* reset index */
      inputBufferIndex = 0
      /* reset used */
      if(verbose) {
        val timeUs = (System.nanoTime() - start) / 1000
        System.err.println(TeraSort.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " HDFS read bytes: " + target +
          " time : " + timeUs + " usec , or " +
          (target.asInstanceOf[Long] * 8) / timeUs + " Mbps ")
      }
      readFromHdfs+=1
    }

    override final def nextKeyValue() : Boolean = {
      //don't use Array[Byte].length for any calculation as we might have a buffer that is bigger than what we asked
      if (processedSoFar >= totalIncomingBytes) {
        return false
      }
      if(inputBufferIndex == inputBufferSize) {
        /* time to refill */
        refillInputBuffer()
      }
      /* now we have to copy out things */
      System.arraycopy(inputBuffer, inputBufferIndex, key, 0, TeraInputFormat.KEY_LEN)
      inputBufferIndex += TeraInputFormat.KEY_LEN
      System.arraycopy(inputBuffer, inputBufferIndex, value,  0, TeraInputFormat.VALUE_LEN)
      inputBufferIndex += TeraInputFormat.VALUE_LEN
      /* and also add to the global counter as the full record processed */
      processedSoFar+=TeraInputFormat.RECORD_LEN
      true
    }

    override final def initialize(split : InputSplit, context : TaskAttemptContext) = {
      val reclen = TeraInputFormat.RECORD_LEN
      val fileSplit = split.asInstanceOf[FileSplit]
      val p : Path = fileSplit.getPath
      val fs : FileSystem = p.getFileSystem(context.getConfiguration)
      fs.setVerifyChecksum(false)
      in = fs.open(p)
      // find the offset to start at a record boundary
      offset = (reclen - (fileSplit.getStart % reclen)) % reclen
      val start = offset + fileSplit.getStart
      in.seek(start)
      //check how much is available in the file, with a full multiple this should be good
      length = Math.min(fileSplit.getLength, in.available()) - offset
      val rem = (start + length)%TeraInputFormat.RECORD_LEN
      val endOffset = if(rem == 0) start + length else (start + length + (TeraInputFormat.RECORD_LEN - rem))
      totalIncomingBytes = (endOffset - start)

      require((totalIncomingBytes % TeraInputFormat.RECORD_LEN) == 0 ,
        " incomingBytes did not alight : " + totalIncomingBytes + " mod : " + (totalIncomingBytes % TeraInputFormat.RECORD_LEN))

      if (key == null) {
        key = new Array[Byte](TeraInputFormat.KEY_LEN)
      }
      if (value == null) {
        value = new Array[Byte](TeraInputFormat.VALUE_LEN)
      }
      inputSerBuffer = BufferCache.getInstance().getByteArrayBuffer(inputBufferSize)
      inputBuffer = inputSerBuffer.getByteArray
      if(verbose){
        System.err.println(TeraSort.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " starting, will process " + totalIncomingBytes + " bytes wih buffer size: " + inputBufferSize)
      }
      refillInputBuffer()
    }


    override final def close() = {
      BufferCache.getInstance().putBuffer(inputSerBuffer)
      in.close()
      if(verbose){
        System.err.println(TeraSort.verbosePrefixHDFSInput + " TID: " + TaskContext.get.taskAttemptId() +
          " finished, processed " + processedSoFar + " bytes, with " +readFromHdfs + " times to HDFS, "
          + BufferCache.getInstance.getCacheStatus)
      }
    }
    override final def getCurrentKey : Array[Byte] = key
    override final def getCurrentValue : Array[Byte] = value
    override final def getProgress : Float = processedSoFar / totalIncomingBytes
  }
}