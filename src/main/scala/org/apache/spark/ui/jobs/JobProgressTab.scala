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

import javax.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.{SparkUI, WebUITab}

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[ui] class JobProgressTab(parent: SparkUI) extends WebUITab(parent, "stages") {
  val appName = parent.appName
  val basePath = parent.basePath
  val live = parent.live
  val sc = parent.sc
  val conf = if (live) sc.conf else new SparkConf
  val killEnabled = conf.getBoolean("spark.ui.killEnabled", true)
  val listener = new JobProgressListener(conf)

  attachPage(new JobProgressPage(this))
  attachPage(new StagePage(this))
  attachPage(new PoolPage(this))
  parent.registerListener(listener)

  def isFairScheduler = listener.schedulingMode.exists(_ == SchedulingMode.FAIR)

  def handleKillRequest(request: HttpServletRequest) =  {
    if (killEnabled) {
      val killFlag = Option(request.getParameter("terminate")).getOrElse("false").toBoolean
      val stageId = Option(request.getParameter("id")).getOrElse("-1").toInt
      if (stageId >= 0 && killFlag && listener.activeStages.contains(stageId)) {
        sc.cancelStage(stageId)
      }
      // Do a quick pause here to give Spark time to kill the stage so it shows up as
      // killed after the refresh. Note that this will block the serving thread so the
      // time should be limited in duration.
      Thread.sleep(100)
    }
  }
}
