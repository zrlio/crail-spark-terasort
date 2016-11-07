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
