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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

private[network]
class MessageChunk(val header: MessageChunkHeader, val buffer: ByteBuffer) {

  val size = if (buffer == null) 0 else buffer.remaining

  lazy val buffers = {
    val ab = new ArrayBuffer[ByteBuffer]()
    ab += header.buffer
    if (buffer != null) {
      ab += buffer
    }
    ab
  }

  override def toString = {
    "" + this.getClass.getSimpleName + " (id = " + header.id + ", size = " + size + ")"
  }
}
