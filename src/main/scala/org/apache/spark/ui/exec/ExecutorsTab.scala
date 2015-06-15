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

package org.apache.spark.ui.exec

import scala.collection.mutable.HashMap

import org.apache.spark.ExceptionFailure
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.{SparkUI, WebUITab}

private[ui] class ExecutorsTab(parent: SparkUI) extends WebUITab(parent, "executors") {
  val appName = parent.appName
  val basePath = parent.basePath
  val listener = new ExecutorsListener(parent.storageStatusListener)

  attachPage(new ExecutorsPage(this))
  parent.registerListener(listener)
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener)
  extends SparkListener {

  val executorToTasksActive = HashMap[String, Int]()
  val executorToTasksComplete = HashMap[String, Int]()
  val executorToTasksFailed = HashMap[String, Int]()
  val executorToDuration = HashMap[String, Long]()
  val executorToShuffleRead = HashMap[String, Long]()
  val executorToShuffleWrite = HashMap[String, Long]()

  def storageStatusList = storageStatusListener.storageStatusList

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val eid = formatExecutorId(taskStart.taskInfo.executorId)
    executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = formatExecutorId(info.executorId)
      executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 1) - 1
      executorToDuration(eid) = executorToDuration.getOrElse(eid, 0L) + info.duration
      taskEnd.reason match {
        case e: ExceptionFailure =>
          executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
        case _ =>
          executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
      }

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        metrics.shuffleReadMetrics.foreach { shuffleRead =>
          executorToShuffleRead(eid) =
            executorToShuffleRead.getOrElse(eid, 0L) + shuffleRead.remoteBytesRead
        }
        metrics.shuffleWriteMetrics.foreach { shuffleWrite =>
          executorToShuffleWrite(eid) =
            executorToShuffleWrite.getOrElse(eid, 0L) + shuffleWrite.shuffleBytesWritten
        }
      }
    }
  }

  // This addresses executor ID inconsistencies in the local mode
  private def formatExecutorId(execId: String) = storageStatusListener.formatExecutorId(execId)
}
