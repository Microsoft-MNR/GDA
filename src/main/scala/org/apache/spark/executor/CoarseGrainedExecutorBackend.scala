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

package org.apache.spark.executor

import java.nio.ByteBuffer

import akka.actor._
import akka.remote._

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{AkkaUtils, Utils}


private[spark] class CoarseGrainedExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: ActorSelection = null

  def readBWFromFile(filename: String): Double = {
    scala.io.Source.fromFile(filename).mkString.toDouble
  }

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorSelection(driverUrl)
    driver ! RegisterExecutor(executorId, hostPort, cores, readBWFromFile("/root/bw.txt"))
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with driver")
      // Make this host instead of hostPort ?
      executor = new Executor(executorId, Utils.parseHostPort(hostPort)._1, sparkProperties,
        false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo(" name " + taskDesc.name + " additioanl: " + taskDesc.additional)
      //+ taskDesc.name + " id" + taskDesc.additional)
      logInfo("Got assigned task " + taskDesc.taskId)

      //executor.updateMOLFromTaskDesc(taskDesc.mapStatus, taskDesc.additional)

      //logInfo("additional bit" + taskDesc.additional)
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
      }

    case UpdateMapOutputExecutor(serializedStatus: Array[Byte], shuffleId: Int, reduceId: Int) =>
      executor.updateMapOutput(serializedStatus, shuffleId, reduceId)

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case x: DisassociatedEvent =>
      logError(s"Driver $x disassociated! Shutting down.")
      System.exit(1)

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      context.stop(self)
      context.system.shutdown()
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object CoarseGrainedExecutorBackend {
  def run(driverUrl: String, executorId: String, hostname: String, cores: Int,
    workerUrl: Option[String]) {

    SparkHadoopUtil.get.runAsSparkUser { () =>
        // Debug code
        Utils.checkHost(hostname)

        val conf = new SparkConf
        // Create a new ActorSystem to run the backend, because we can't create a
        // SparkEnv / Executor before getting started with all our system properties, etc
        val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0,
          conf, new SecurityManager(conf))
        // set it
        val sparkHostPort = hostname + ":" + boundPort
        actorSystem.actorOf(
          Props(classOf[CoarseGrainedExecutorBackend], driverUrl, executorId,
            sparkHostPort, cores),
          name = "Executor")
        workerUrl.foreach {
          url =>
            actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
        }
        actorSystem.awaitTermination()

    }
  }

  def main(args: Array[String]) {
    args.length match {
      case x if x < 4 =>
        System.err.println(
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          "Usage: CoarseGrainedExecutorBackend <driverUrl> <executorId> <hostname> " +
          "<cores> [<workerUrl>]")
        System.exit(1)
      case 4 =>
        run(args(0), args(1), args(2), args(3).toInt, None)
      case x if x > 4 =>
        run(args(0), args(1), args(2), args(3).toInt, Some(args(4)))
    }
  }
}
