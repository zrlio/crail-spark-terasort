package com.ibm.crail.terasort

/**
  * Created by atr on 07.11.16.
  */
object TeraConf {
  /* input conf */
  val INPUT_KEY_LEN = 10
  val INPUT_VALUE_LEN = 90
  val INPUT_RECORD_LEN = INPUT_KEY_LEN + INPUT_VALUE_LEN

  /* spark keys for conf */
  val f22BufSizeKey = "spark.terasort.f22buffersize"
  val kryoBufSizeKey = "spark.terasort.kryobuffersize"
  val verboseKey = "spark.terasort.verbose"
  val inputBufferSizeKey = "spark.terasort.inputbuffersize"
  val outputBufferSizeKey = "spark.terasort.outputbuffersize"
  val useBigIteratorKey = "spark.terasort.usebigiterator"

  /* debug properties */
  val verbosePrefixF22 = "F22 | "
  val verbosePrefixSorter = "Sorter | "
  val verbosePrefixIterator = "SorterIterator | "
  val verbosePrefixCache = "Cache | "
  val verbosePrefixHDFSInput = "HDFSInput | "
  val verbosePrefixHDFSOutput = "HDFSOutput | "

  /* Hahoop job output conf */
  val OUTPUT_SYNC_ATTRIBUTE = "mapreduce.terasort.final.sync"
  val OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir"
}
