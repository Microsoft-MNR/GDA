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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{TimerTask, Timer}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils
import scala.util.control.Breaks._
/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  val BANDWIDTH_SCHEDULING = conf.getBoolean("spark.dag.bandwidthScheduling", false)
  val LP_BANDWIDTH_SCHEDULING = conf.getBoolean("spark.dag.lpBandwidthScheduling", false)

  //val BANDWIDTH_FILE_NAME = conf.get("spark.dag.bandwidthFileName", "\\root\\bwconf.txt")
  val SITE_INFO_FILE_NAME = conf.get("spark.dag.siteInfoFileName", "\\root\\site.txt")
  val MAP_OUTPUT_INFO_FILE_NAME = conf.get("spark.dag.mapOutputInfoFileName", "\\root\\mapooutput.txt")
  val PLACEMENT_FILE_NAME = conf.get("spark.dag.placementFileName", "\\root\\placement.txt")

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  val activeTaskSets = new HashMap[String, TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  private val executorsByHost = new HashMap[String, HashSet[String]]

  private val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      import sc.env.actorSystem.dispatcher
      sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
            SPECULATION_INTERVAL milliseconds) {
        Utils.tryOrExit { checkSpeculatableTasks() }
      }
    }
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    for (task <- tasks) {
      logInfo("Adding task shuffle ID " + task.shuffleId)
    }

    this.synchronized {
      val manager = new TaskSetManager(this, taskSet, maxTaskFailures)
      activeTaskSets(taskSet.id) = manager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient memory")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
      // There are two possible cases here:
      // 1. The task set manager has been created and some tasks have been scheduled.
      //    In this case, send a kill signal to the executors to kill the task and then abort
      //    the stage.
      // 2. The task set manager has been created but no tasks has been scheduled. In this case,
      //    simply abort the stage.
      tsm.runningTasksSet.foreach { tid =>
        val execId = taskIdToExecutorId(tid)
        backend.killTask(tid, execId, interruptThread)
      }
      tsm.abort("Stage %s cancelled".format(stageId))
      logInfo("Stage %d was cancelled".format(stageId))
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    activeTaskSets -= manager.taskSet.id
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    SparkEnv.set(sc.env)

    // Mark each slave as alive and remember its hostname
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- TaskLocality.values) {
      do {
        launchedTask = false
        for (i <- 0 until shuffledOffers.size) {
          val execId = shuffledOffers(i).executorId
          val host = shuffledOffers(i).host
          if (availableCpus(i) >= CPUS_PER_TASK) {
            for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
              tasks(i) += task
              val tid = task.taskId
              taskIdToTaskSetId(tid) = taskSet.taskSet.id
              taskIdToExecutorId(tid) = execId
              activeExecutorIds += execId
              executorsByHost(host) += execId
              availableCpus(i) -= CPUS_PER_TASK
              assert (availableCpus(i) >= 0)
              launchedTask = true
              logInfo("Task " + task.taskId + " from stage " + taskSet.stageId + " goes to exec "  + execId)
            }
          }
        }
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  /** code for bw-aware LP
   *
   */
  def lpBWAware(shuffledOffers: Seq[WorkerOfferWithBW],
                       tasks: Seq[ArrayBuffer[TaskDescription]],
                       availableCpus: Array[Int],
                       taskSet: TaskSetManager) {
    // write map ouput info
    val mapInfo: Array[MapStatus] = taskSet.taskSet.getMapTaskInfo()
    var mapFile = new java.io.FileOutputStream(MAP_OUTPUT_INFO_FILE_NAME)
    var mapStream = new java.io.PrintStream(mapFile)
    var nReducer = 0
    for (status <- mapInfo) {
      val host = status.location.host
      mapStream.print(host)
      val sizeList = status.compressedSizes.map(MapOutputTracker.decompressSize)
      nReducer = sizeList.size
      for (size <- sizeList) {
        mapStream.print(" " + size.toString())
      }
      mapStream.print("\n")
    }
    mapStream.close()
    // write site info
    var siteFile = new java.io.FileOutputStream(SITE_INFO_FILE_NAME)
    var siteStream = new java.io.PrintStream(siteFile)
    val hostExecMap = new HashMap[String, String]
    val hostOfferMap = new HashMap[String, Int]
    for (i <- 0 until shuffledOffers.size) {
      val host = shuffledOffers(i).host
      val execId = shuffledOffers(i).executorId
      siteStream.print(host + " " +  shuffledOffers(i).bw + " " + shuffledOffers(i).bw + "\n")
      hostExecMap.put(host, execId)
      hostOfferMap.put(host, i)
    }
    siteStream.close()
    siteFile.close()
    // issue the command
    import sys.process._
    "Rscript /root/bw-aware.r" !
    // read the output
    val placeFile = new java.io.FileInputStream(PLACEMENT_FILE_NAME)
    val placeReader = new java.io.BufferedReader(new java.io.InputStreamReader(placeFile))
    val placement = new Array[String](nReducer)
    for (i <- 0 to nReducer-1) {
      placement(i) = placeReader.readLine()
      logInfo("reducer " + i + " assigned to " + placement(i))
    }
    val time = placeReader.readLine()
    logInfo("total time " + time)
    logInfo("assuming CPU is not bottleneck now, TODO: add constraits into solver")
    placeReader.close()
    placeFile.close()

    // do the actual scheduling now
    var launchedTask = false
    val maxLocality = TaskLocality.ANY
    do {
      launchedTask = false
      for (taskIndex <- 0 to placement.size - 1) {
        val host = placement(taskIndex)
        val execId = hostExecMap.getOrElse(host, "0")
        val i = hostOfferMap.getOrElse(host, 0)
        if (availableCpus(i) >= CPUS_PER_TASK) {
          val taskGenerated = taskSet.resourceOfferWithIndex(execId, host, taskIndex)

          for (task <- taskGenerated) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetId(tid) = taskSet.taskSet.id
            taskIdToExecutorId(tid) = execId
            activeExecutorIds += execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
            logInfo("BW aware scheduling Task " + task.taskId + " from stage " + taskSet.stageId + " goes to exec " + execId)
          }
        }
      }
    } while (launchedTask)
  }

    /** code for performing bw-aware heuristic
   *
   */
  def heuristicBWAware(shuffledOffers: Seq[WorkerOfferWithBW],
                       tasks: Seq[ArrayBuffer[TaskDescription]],
                       availableCpus: Array[Int],
                       taskSet: TaskSetManager) {
    var launchedTask = false
    //reducer taskset
    //1 get location of all map tasks (assume they generate same amount of data
    val mapInfo: Array[MapStatus] = taskSet.taskSet.getMapTaskInfo()
    logInfo("assuming totally uniform key distribution here, assuming one executor/host")
    //2 get number of reducers
    val numTasks = taskSet.numTasks
    logInfo("number of tasks: " + numTasks)
    //3 get bandwidth
    val hostBWMap = new HashMap[String, Float]
    val hostExecMap = new HashMap[String, String]
    val hostOfferMap = new HashMap[String, Int]
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      logInfo("resources: e: " + execId + " h: " + host + " bw: " + shuffledOffers(i).bw)
      hostBWMap.put(host, shuffledOffers(i).bw.toFloat)
      hostExecMap.put(host, execId)
      hostOfferMap.put(host, i)
    }
    val hostSizeMap = new HashMap[String, Long]
    for (status <- mapInfo) {
      val host = status.location.host
      val totalSize = status.compressedSizes.map(MapOutputTracker.decompressSize).sum
      logInfo("map location " + host
        + " size: " + totalSize)
      hostSizeMap.put(host, totalSize + hostSizeMap.getOrElse(host, 0L))
      //host = status.location.host
    }

    var hostSubset = List[String]()
    var candidateTime = List[Float]()
    val totalDataSize = hostSizeMap.values.sum
    for (pair <- hostBWMap.toSeq.sortBy(_._2).reverse) {
      logInfo("trying different subse size....")
      hostSubset :+= pair._1
      var maxTime = 0.0f
      for (host <- hostSizeMap.keys) {
        if (hostSubset.contains(host)) {
          val timeIn = 1.0f * (totalDataSize - hostSizeMap(host)) / hostSubset.size / hostBWMap(host)
          val timeOut = 1.0f * hostSizeMap(host) / hostSubset.size * (hostSubset.size - 1) / hostBWMap(host)
          maxTime = List(maxTime, timeIn, timeOut).max
        } else {
          val timeOut = 1.0f * hostSizeMap(host) / hostBWMap(host)
          maxTime = List(maxTime, timeOut).max
        }
      }
      candidateTime :+= maxTime
    }
    val nHosts = candidateTime.zipWithIndex.minBy(_._1)._2 + 1
    val desiredHosts = hostBWMap.toSeq.sortBy(_._2).reverse.map(x => x._1).slice(0, nHosts).toList
    logInfo("ideal list: " + desiredHosts)
    // fake decision, half goes to B, half goes to C
    var numAllocatedTasks = 0
    // go to any directly
    //for (maxLocality <- TaskLocality.ANY) {
    val maxLocality = TaskLocality.ANY
    do {
      launchedTask = false
      for (host <- desiredHosts) {
        val execId = hostExecMap.getOrElse(host, "0")
        val i = hostOfferMap.getOrElse(host, 0)
        if (availableCpus(i) >= CPUS_PER_TASK) {
          val taskGenerated = taskSet.resourceOffer(execId, host, maxLocality)
          /*
            logInfo("host " + host + " exec " + execId + " "
              + numAllocatedTasks + "/" + numTasks
              + " locality: " + maxLocality)
            logInfo("taskgenerated " + taskGenerated.size)
          */
          for (task <- taskGenerated) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetId(tid) = taskSet.taskSet.id
            taskIdToExecutorId(tid) = execId
            activeExecutorIds += execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
            numAllocatedTasks += 1
            logInfo("BW aware scheduling Task " + task.taskId + " from stage " + taskSet.stageId + " goes to exec " + execId)
          }
        }
      }
    } while (launchedTask)
  }
  /**
   * functionally similar to resourceOffers, also takes bandwidth information
   *
   */
  def resourceOffersWithBW(offers: Seq[WorkerOfferWithBW]): Seq[Seq[TaskDescription]] = synchronized {
    SparkEnv.set(sc.env)

    // Mark each slave as alive and remember its hostname
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("in resourceOffersWithBW parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
    }
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    var launchedTask = false
    for (taskSet <- sortedTaskSets) {
      if (taskSet.taskSet.checkShuffleDependent() && (BANDWIDTH_SCHEDULING || LP_BANDWIDTH_SCHEDULING)) {
        if (LP_BANDWIDTH_SCHEDULING) {
          // lp logic starts here
          lpBWAware(shuffledOffers, tasks, availableCpus, taskSet)
          // lp logic ends
        } else {
          //heuristic logic starts here
          heuristicBWAware(shuffledOffers, tasks, availableCpus, taskSet)
          //heuristic logic ends
        }
      } else {
        for (maxLocality <- TaskLocality.values) {
          do {
            launchedTask = false
            for (i <- 0 until shuffledOffers.size) {
              val execId = shuffledOffers(i).executorId
              val host = shuffledOffers(i).host
              if (availableCpus(i) >= CPUS_PER_TASK) {
                for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
                  tasks(i) += task
                  val tid = task.taskId
                  taskIdToTaskSetId(tid) = taskSet.taskSet.id
                  taskIdToExecutorId(tid) = execId
                  activeExecutorIds += execId
                  executorsByHost(host) += execId
                  availableCpus(i) -= CPUS_PER_TASK
                  assert(availableCpus(i) >= 0)
                  launchedTask = true
                  logInfo("Task " + task.taskId + " from stage " + taskSet.stageId + " goes to exec " + execId)
                }
              }
            }
          } while (launchedTask)
        }
      }
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }


  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              taskIdToExecutorId.remove(tid)
            }
            activeTaskSets.get(taskSetId).foreach { taskSet =>
              if (state == TaskState.FINISHED) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
               "likely the result of receiving duplicate task finished status updates)")
              .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long) {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]) = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: TaskEndReason) = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (activeTaskSets.size > 0) {
        // Have each task set throw a SparkException with the error
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()

    // sleeping for an arbitrary 1 seconds to ensure that messages are sent out.
    Thread.sleep(1000L)
  }

  override def defaultParallelism() = backend.defaultParallelism()

  override def updateMapOutput(eId: String,
    shuffleId: Int,
    statuses: Array[Byte],
    index: Int) {
    backend.updateMapOutput(eId, shuffleId, statuses, index)
  }

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None
}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
