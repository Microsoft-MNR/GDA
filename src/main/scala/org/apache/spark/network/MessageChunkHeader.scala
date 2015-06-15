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

package org.apache.spark.network

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import org.apache.spark.Logging

private[spark] class MessageChunkHeader(
    val typ: Long,
    val id: Int,
    val totalSize: Int,
    val chunkSize: Int,
    val other: Int,
    val securityNeg: Int,
    val address: InetSocketAddress,
    val blockName: String)  extends Logging {

  lazy val buffer = {
    // No need to change this, at 'use' time, we do a reverse lookup of the hostname.
    // Refer to network.Connection
    val ip = address.getAddress.getAddress()
    val port = address.getPort()
    if (blockName.size > 20) {
      throw new Exception("block name size too big")
    }
    //logInfo("Create message chunk header , ipsize " + ip.size)
    ByteBuffer.
      allocate(MessageChunkHeader.HEADER_SIZE).
      putLong(typ).
      putInt(id).
      putInt(totalSize).
      putInt(chunkSize).
      putInt(other).
      putInt(securityNeg).
      putInt(ip.size).
      put(ip).
      putInt(port).
      putInt(blockName.size).
      put(blockName.toCharArray.map(_.toByte)).
      position(MessageChunkHeader.HEADER_SIZE).
      flip.asInstanceOf[ByteBuffer]
  }

  override def toString = "" + this.getClass.getSimpleName + ":" + id + " of type " + typ +
      " and sizes " + totalSize + " / " + chunkSize + " bytes, securityNeg: " + securityNeg +
      " blockName " + blockName
}


private[spark] object MessageChunkHeader extends Logging {
  //val HEADER_SIZE = 44
  val HEADER_SIZE = 68

  def create(buffer: ByteBuffer): MessageChunkHeader = {
    if (buffer.remaining != HEADER_SIZE) {
      throw new IllegalArgumentException("Cannot convert buffer data to Message")
    }
    val typ = buffer.getLong()
    val id = buffer.getInt()
    val totalSize = buffer.getInt()
    val chunkSize = buffer.getInt()
    val other = buffer.getInt()
    val securityNeg = buffer.getInt()
    val ipSize = buffer.getInt()
    val ipBytes = new Array[Byte](ipSize)
    buffer.get(ipBytes)
    val ip = InetAddress.getByAddress(ipBytes)
    val port = buffer.getInt()
    val blockNameSize = buffer.getInt()
    val blockNameArray = new Array[Byte](blockNameSize)
    if (blockNameSize > 20) {
      throw new Exception("block name size too big")
    }
    buffer.get(blockNameArray)
    val blockName = new String(blockNameArray.map(_.toChar))
    new MessageChunkHeader(typ, id, totalSize, chunkSize, other, securityNeg,
      new InetSocketAddress(ip, port), blockName)
  }
}
