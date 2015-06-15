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

package org.apache.spark.ui.jobs

import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.{Utils, Distribution}

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: JobProgressTab) extends WebUIPage("stage") {
  private val appName = parent.appName
  private val basePath = parent.basePath
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt

      if (!listener.stageIdToTaskData.contains(stageId)) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return UIUtils.headerSparkPage(content, basePath, appName,
          "Details for Stage %s".format(stageId), parent.headerTabs, parent)
      }

      val tasks = listener.stageIdToTaskData(stageId).values.toSeq.sortBy(_.taskInfo.launchTime)

      val numCompleted = tasks.count(_.taskInfo.finished)
      val shuffleReadBytes = listener.stageIdToShuffleRead.getOrElse(stageId, 0L)
      val hasShuffleRead = shuffleReadBytes > 0
      val shuffleWriteBytes = listener.stageIdToShuffleWrite.getOrElse(stageId, 0L)
      val hasShuffleWrite = shuffleWriteBytes > 0
      val memoryBytesSpilled = listener.stageIdToMemoryBytesSpilled.getOrElse(stageId, 0L)
      val diskBytesSpilled = listener.stageIdToDiskBytesSpilled.getOrElse(stageId, 0L)
      val hasBytesSpilled = memoryBytesSpilled > 0 && diskBytesSpilled > 0

      var activeTime = 0L
      val now = System.currentTimeMillis
      val tasksActive = listener.stageIdToTasksActive(stageId).values
      tasksActive.foreach(activeTime += _.timeRunning(now))

      // scalastyle:off
      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total task time across all tasks: </strong>
              {UIUtils.formatDuration(listener.stageIdToTime.getOrElse(stageId, 0L) + activeTime)}
            </li>
            {if (hasShuffleRead)
              <li>
                <strong>Shuffle read: </strong>
                {Utils.bytesToString(shuffleReadBytes)}
              </li>
            }
            {if (hasShuffleWrite)
              <li>
                <strong>Shuffle write: </strong>
                {Utils.bytesToString(shuffleWriteBytes)}
              </li>
            }
            {if (hasBytesSpilled)
            <li>
              <strong>Shuffle spill (memory): </strong>
              {Utils.bytesToString(memoryBytesSpilled)}
            </li>
            <li>
              <strong>Shuffle spill (disk): </strong>
              {Utils.bytesToString(diskBytesSpilled)}
            </li>
            }
          </ul>
        </div>
        // scalastyle:on
      val taskHeaders: Seq[String] =
        Seq("Task Index", "Task ID", "Status", "Locality Level", "Executor", "Launch Time") ++
        Seq("Duration", "GC Time", "Result Ser Time") ++
        {if (hasShuffleRead) Seq("Shuffle Read")  else Nil} ++
        {if (hasShuffleWrite) Seq("Write Time", "Shuffle Write") else Nil} ++
        {if (hasBytesSpilled) Seq("Shuffle Spill (Memory)", "Shuffle Spill (Disk)") else Nil} ++
        Seq("Errors")

      val taskTable = UIUtils.listingTable(
        taskHeaders, taskRow(hasShuffleRead, hasShuffleWrite, hasBytesSpilled), tasks)

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.taskMetrics.isDefined)

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          val serializationTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.resultSerializationTime.toDouble
          }
          val serializationQuantiles =
            "Result serialization time" +: Distribution(serializationTimes).
              get.getQuantiles().map(ms => UIUtils.formatDuration(ms.toLong))

          val serviceTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.executorRunTime.toDouble
          }
          val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles()
            .map(ms => UIUtils.formatDuration(ms.toLong))

          val gettingResultTimes = validTasks.map { case TaskUIData(info, _, _) =>
            if (info.gettingResultTime > 0) {
              (info.finishTime - info.gettingResultTime).toDouble
            } else {
              0.0
            }
          }
          val gettingResultQuantiles = "Time spent fetching task results" +:
            Distribution(gettingResultTimes).get.getQuantiles().map { millis =>
              UIUtils.formatDuration(millis.toLong)
            }
          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map { case TaskUIData(info, metrics, _) =>
            val totalExecutionTime = {
              if (info.gettingResultTime > 0) {
                (info.gettingResultTime - info.launchTime).toDouble
              } else {
                (info.finishTime - info.launchTime).toDouble
              }
            }
            totalExecutionTime - metrics.get.executorRunTime
          }
          val schedulerDelayQuantiles = "Scheduler delay" +:
            Distribution(schedulerDelays).get.getQuantiles().map { millis =>
              UIUtils.formatDuration(millis.toLong)
            }

          def getQuantileCols(data: Seq[Double]) =
            Distribution(data).get.getQuantiles().map(d => Utils.bytesToString(d.toLong))

          val shuffleReadSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

          val shuffleWriteSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
          }
          val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

          val memoryBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = "Shuffle spill (memory)" +:
            getQuantileCols(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = "Shuffle spill (disk)" +:
            getQuantileCols(diskBytesSpilledSizes)

          val listings: Seq[Seq[String]] = Seq(
            serializationQuantiles,
            serviceQuantiles,
            gettingResultQuantiles,
            schedulerDelayQuantiles,
            if (hasShuffleRead) shuffleReadQuantiles else Nil,
            if (hasShuffleWrite) shuffleWriteQuantiles else Nil,
            if (hasBytesSpilled) memoryBytesSpilledQuantiles else Nil,
            if (hasBytesSpilled) diskBytesSpilledQuantiles else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
          Some(UIUtils.listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true))
        }
      val executorTable = new ExecutorTable(stageId, parent)
      val content =
        summary ++
        <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>Aggregated Metrics by Executor</h4> ++ executorTable.toNodeSeq ++
        <h4>Tasks</h4> ++ taskTable

      UIUtils.headerSparkPage(content, basePath, appName, "Details for Stage %d".format(stageId),
        parent.headerTabs, parent)
    }
  }

  def taskRow(shuffleRead: Boolean, shuffleWrite: Boolean, bytesSpilled: Boolean)
      (taskData: TaskUIData): Seq[Node] = {
    def fmtStackTrace(trace: Seq[StackTraceElement]): Seq[Node] =
      trace.map(e => <span style="display:block;">{e.toString}</span>)

    taskData match { case TaskUIData(info, metrics, exception) =>
      val duration = if (info.status == "RUNNING") info.timeRunning(System.currentTimeMillis())
        else metrics.map(_.executorRunTime).getOrElse(1L)
      val formatDuration = if (info.status == "RUNNING") UIUtils.formatDuration(duration)
        else metrics.map(m => UIUtils.formatDuration(m.executorRunTime)).getOrElse("")
      val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
      val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)

      val maybeShuffleRead = metrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead)
      val shuffleReadSortable = maybeShuffleRead.map(_.toString).getOrElse("")
      val shuffleReadReadable = maybeShuffleRead.map(Utils.bytesToString).getOrElse("")

      val maybeShuffleWrite =
        metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten)
      val shuffleWriteSortable = maybeShuffleWrite.map(_.toString).getOrElse("")
      val shuffleWriteReadable = maybeShuffleWrite.map(Utils.bytesToString).getOrElse("")

      val maybeWriteTime = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleWriteTime)
      val writeTimeSortable = maybeWriteTime.map(_.toString).getOrElse("")
      val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
        if (ms == 0) "" else UIUtils.formatDuration(ms)
      }.getOrElse("")

      val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
      val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.map(_.toString).getOrElse("")
      val memoryBytesSpilledReadable =
        maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

      val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
      val diskBytesSpilledSortable = maybeDiskBytesSpilled.map(_.toString).getOrElse("")
      val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

      <tr>
        <td>{info.index}</td>
        <td>{info.taskId}</td>
        <td>{info.status}</td>
        <td>{info.taskLocality}</td>
        <td>{info.host}</td>
        <td>{UIUtils.formatDate(new Date(info.launchTime))}</td>
        <td sorttable_customkey={duration.toString}>
          {formatDuration}
        </td>
        <td sorttable_customkey={gcTime.toString}>
          {if (gcTime > 0) UIUtils.formatDuration(gcTime) else ""}
        </td>
        <td sorttable_customkey={serializationTime.toString}>
          {if (serializationTime > 0) UIUtils.formatDuration(serializationTime) else ""}
        </td>
        {if (shuffleRead) {
           <td sorttable_customkey={shuffleReadSortable}>
             {shuffleReadReadable}
           </td>
        }}
        {if (shuffleWrite) {
           <td sorttable_customkey={writeTimeSortable}>
             {writeTimeReadable}
           </td>
           <td sorttable_customkey={shuffleWriteSortable}>
             {shuffleWriteReadable}
           </td>
        }}
        {if (bytesSpilled) {
          <td sorttable_customkey={memoryBytesSpilledSortable}>
            {memoryBytesSpilledReadable}
          </td>
          <td sorttable_customkey={diskBytesSpilledSortable}>
            {diskBytesSpilledReadable}
          </td>
        }}
        <td>
          {exception.map { e =>
            <span>
              {e.className} ({e.description})<br/>
              {fmtStackTrace(e.stackTrace)}
            </span>
          }.getOrElse("")}
        </td>
      </tr>
    }
  }
}
