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

package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import tachyon.client.{WriteType, ReadType}

import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.apache.spark.serializer.Serializer


private class Entry(val size: Long)


/**
 * Stores BlockManager blocks on Tachyon.
 */
private class TachyonStore(
    blockManager: BlockManager,
    tachyonManager: TachyonBlockManager)
  extends BlockStore(blockManager: BlockManager) with Logging {

  logInfo("TachyonStore started")

  override def getSize(blockId: BlockId): Long = {
    tachyonManager.getFile(blockId.name).length
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult =  {
    putToTachyonStore(blockId, bytes, true)
  }

  override def putValues(
      blockId: BlockId,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    return putValues(blockId, values.toIterator, level, returnValues)
  }

  override def putValues(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    logDebug("Attempting to write values for block " + blockId)
    val _bytes = blockManager.dataSerialize(blockId, values)
    putToTachyonStore(blockId, _bytes, returnValues)
  }

  private def putToTachyonStore(
      blockId: BlockId,
      bytes: ByteBuffer,
      returnValues: Boolean): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val byteBuffer = bytes.duplicate()
    byteBuffer.rewind()
    logDebug("Attempting to put block " + blockId + " into Tachyon")
    val startTime = System.currentTimeMillis
    val file = tachyonManager.getFile(blockId)
    val os = file.getOutStream(WriteType.TRY_CACHE)
    os.write(byteBuffer.array())
    os.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file in Tachyon in %d ms".format(
      blockId, Utils.bytesToString(byteBuffer.limit), (finishTime - startTime)))

    if (returnValues) {
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    } else {
      PutResult(bytes.limit(), null)
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    val file = tachyonManager.getFile(blockId)
    if (tachyonManager.fileExists(file)) {
      tachyonManager.removeFile(file)
    } else {
      false
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }


  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = tachyonManager.getFile(blockId)
    if (file == null || file.getLocationHosts().size == 0) {
      return None
    }
    val is = file.getInStream(ReadType.CACHE)
    var buffer: ByteBuffer = null
    try {
      if (is != null) {
        val size = file.length
        val bs = new Array[Byte](size.asInstanceOf[Int])
        val fetchSize = is.read(bs, 0, size.asInstanceOf[Int])
        buffer = ByteBuffer.wrap(bs)
        if (fetchSize != size) {
          logWarning("Failed to fetch the block " + blockId + " from Tachyon : Size " + size +
            " is not equal to fetched size " + fetchSize)
          return None
        }
      }
    } catch {
        case ioe: IOException => {
          logWarning("Failed to fetch the block " + blockId + " from Tachyon", ioe)
          return None
        }
    }
    Some(buffer)
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = tachyonManager.getFile(blockId)
    tachyonManager.fileExists(file)
  }
}
