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

/**
 * These are exit codes that executors should use to provide the master with information about
 * executor failures assuming that cluster management framework can capture the exit codes (but
 * perhaps not log files). The exit code constants here are chosen to be unlikely to conflict
 * with "natural" exit statuses that may be caused by the JVM or user code. In particular,
 * exit codes 128+ arise on some Unix-likes as a result of signals, and it appears that the
 * OpenJDK JVM may use exit code 1 in some of its own "last chance" code.
 */
private[spark]
object ExecutorExitCode {
  /** The default uncaught exception handler was reached. */
  val UNCAUGHT_EXCEPTION = 50

  /** The default uncaught exception handler was called and an exception was encountered while
      logging the exception. */
  val UNCAUGHT_EXCEPTION_TWICE = 51

  /** The default uncaught exception handler was reached, and the uncaught exception was an
      OutOfMemoryError. */
  val OOM = 52

  /** DiskStore failed to create a local temporary directory after many attempts. */
  val DISK_STORE_FAILED_TO_CREATE_DIR = 53

  /** TachyonStore failed to initialize after many attempts. */
  val TACHYON_STORE_FAILED_TO_INITIALIZE = 54

  /** TachyonStore failed to create a local temporary directory after many attempts. */
  val TACHYON_STORE_FAILED_TO_CREATE_DIR = 55

  def explainExitCode(exitCode: Int): String = {
    exitCode match {
      case UNCAUGHT_EXCEPTION => "Uncaught exception"
      case UNCAUGHT_EXCEPTION_TWICE => "Uncaught exception, and logging the exception failed"
      case OOM => "OutOfMemoryError"
      case DISK_STORE_FAILED_TO_CREATE_DIR =>
        "Failed to create local directory (bad spark.local.dir?)"
      case TACHYON_STORE_FAILED_TO_INITIALIZE => "TachyonStore failed to initialize."
      case TACHYON_STORE_FAILED_TO_CREATE_DIR =>
        "TachyonStore failed to create a local temporary directory."
      case _ =>
        "Unknown executor exit code (" + exitCode + ")" + (
          if (exitCode > 128) {
            " (died from signal " + (exitCode - 128) + "?)"
          } else {
            ""
          }
        )
    }
  }
}
