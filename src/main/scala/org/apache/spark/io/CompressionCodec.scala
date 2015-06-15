/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.io

import java.io.{InputStream, OutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 *
 * Note: The wire protocol for a codec is not guaranteed compatible across versions of Spark.
 *       This is intended for use as an internal compression utility within a single
 *       Spark application.
 */
@DeveloperApi
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}


private[spark] object CompressionCodec {
  def createCodec(conf: SparkConf): CompressionCodec = {
    createCodec(conf, conf.get("spark.io.compression.codec", DEFAULT_COMPRESSION_CODEC))
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val ctor = Class.forName(codecName, true, Utils.getContextOrSparkClassLoader)
      .getConstructor(classOf[SparkConf])
    ctor.newInstance(conf).asInstanceOf[CompressionCodec]
  }

  val DEFAULT_COMPRESSION_CODEC = classOf[LZFCompressionCodec].getName
}


/**
 * :: DeveloperApi ::
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * :: DeveloperApi ::
 * Snappy implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by spark.io.compression.snappy.block.size.
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("spark.io.compression.snappy.block.size", 32768)
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}
