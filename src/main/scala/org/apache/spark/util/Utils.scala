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

package org.apache.spark.util

import java.io._
import java.net.{InetAddress, Inet4Address, NetworkInterface, URI, URL, URLConnection}
import java.nio.ByteBuffer
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadPoolExecutor}

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}

import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.json4s._
import tachyon.client.{TachyonFile,TachyonFS}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.ExecutorUncaughtExceptionHandler
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

/**
 * Various utility methods used by Spark.
 */
private[spark] object Utils extends Logging {
  val random = new Random()

  def sparkBin(sparkHome: String, which: String): File = {
    val suffix = if (isWindows) ".cmd" else ""
    new File(sparkHome + File.separator + "bin", which + suffix)
  }

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize a Long value (used for {@link org.apache.spark.api.python.PythonPartitioner}) */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /** Serialize via nested stream using specific serializer */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit) = {
    val osWrapper = ser.serializeStream(new OutputStream {
      def write(b: Int) = os.write(b)

      override def write(b: Array[Byte], off: Int, len: Int) = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /** Deserialize via nested stream using specific serializer */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit) = {
    val isWrapper = ser.deserializeStream(new InputStream {
      def read(): Int = is.read()

      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrSparkClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    Try { Class.forName(clazz, false, getContextOrSparkClassLoader) }.isSuccess
  }

  /**
   * Primitive often used when writing {@link java.nio.ByteBuffer} to {@link java.io.DataOutput}.
   */
  def writeByteBuffer(bb: ByteBuffer, out: ObjectOutput) = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /** Split a string into words at non-alphabetic characters */
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j)
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    buf
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()
  private val shutdownDeleteTachyonPaths = new scala.collection.mutable.HashSet[String]()

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Register the tachyon path to be deleted via shutdown hook
  def registerShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths += absolutePath
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteTachyonDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in Exception and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    val retval = shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /** Create a temporary directory inside the given parent directory */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }

    registerShutdownDeleteDir(dir)

    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        // Attempt to delete if some patch which is parent of this is not already registered.
        if (! hasRootAsShutdownDeleteDir(dir)) Utils.deleteRecursively(dir)
      }
    })
    dir
  }

  /** Copy all data from an InputStream to an OutputStream */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  /**
   * Construct a URI container information used for authentication.
   * This also sets the default authenticator to properly negotiation the
   * user/password based on the URI.
   *
   * Note this relies on the Authenticator.setDefault being set properly to decode
   * the user name and password. This is currently set in the SecurityManager.
   */
  def constructURIForAuthentication(uri: URI, securityMgr: SecurityManager): URI = {
    val userCred = securityMgr.getSecretKey()
    if (userCred == null) throw new Exception("Secret key is null with authentication on")
    val userInfo = securityMgr.getHttpUser()  + ":" + userCred
    new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(),
      uri.getQuery(), uri.getFragment())
  }

  /**
   * Download a file requested by the executor. Supports fetching the file in a variety of ways,
   * including HTTP, HDFS and files on a standard filesystem, based on the URL parameter.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   */
  def fetchFile(url: String, targetDir: File, conf: SparkConf, securityMgr: SecurityManager) {
    val filename = url.split("/").last
    val tempDir = getLocalDir(conf)
    val tempFile =  File.createTempFile("fetchFileTemp", null, new File(tempDir))
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", false)
    uri.getScheme match {
      case "http" | "https" | "ftp" =>
        logInfo("Fetching " + url + " to " + tempFile)

        var uc: URLConnection = null
        if (securityMgr.isAuthenticationEnabled()) {
          logDebug("fetchFile with security enabled")
          val newuri = constructURIForAuthentication(uri, securityMgr)
          uc = newuri.toURL().openConnection()
          uc.setAllowUserInteraction(false)
        } else {
          logDebug("fetchFile not using security")
          uc = new URL(url).openConnection()
        }

        val timeout = conf.getInt("spark.files.fetchTimeout", 60) * 1000
        uc.setConnectTimeout(timeout)
        uc.setReadTimeout(timeout)
        uc.connect()
        val in = uc.getInputStream()
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          if (fileOverwrite) {
            targetFile.delete()
            logInfo(("File %s exists and does not match contents of %s, " +
              "replacing it with %s").format(targetFile, url, url))
          } else {
            tempFile.delete()
            throw new SparkException(
              "File " + targetFile + " exists and does not match contents of" + " " + url)
          }
        }
        Files.move(tempFile, targetFile)
      case "file" | null =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(url)
        var shouldCopy = true
        if (targetFile.exists) {
          if (!Files.equal(sourceFile, targetFile)) {
            if (fileOverwrite) {
              targetFile.delete()
              logInfo(("File %s exists and does not match contents of %s, " +
                "replacing it with %s").format(targetFile, url, url))
            } else {
              throw new SparkException(
                "File " + targetFile + " exists and does not match contents of" + " " + url)
            }
          } else {
            // Do nothing if the file contents are the same, i.e. this file has been copied
            // previously.
            logInfo(sourceFile.getAbsolutePath + " has been previously copied to "
              + targetFile.getAbsolutePath)
            shouldCopy = false
          }
        }

        if (shouldCopy) {
          // The file does not exist in the target directory. Copy it there.
          logInfo("Copying " + sourceFile.getAbsolutePath + " to " + targetFile.getAbsolutePath)
          Files.copy(sourceFile, targetFile)
        }
      case _ =>
        // Use the Hadoop filesystem library, which supports file://, hdfs://, s3://, and others
        val fs = getHadoopFileSystem(uri)
        val in = fs.open(new Path(uri))
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          if (fileOverwrite) {
            targetFile.delete()
            logInfo(("File %s exists and does not match contents of %s, " +
              "replacing it with %s").format(targetFile, url, url))
          } else {
            tempFile.delete()
            throw new SparkException(
              "File " + targetFile + " exists and does not match contents of" + " " + url)
          }
        }
        Files.move(tempFile, targetFile)
    }
    // Decompress the file if it's a .tar or .tar.gz
    if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xzf", filename), targetDir)
    } else if (filename.endsWith(".tar")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xf", filename), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")
  }

  /**
   * Get a temporary directory using Spark's spark.local.dir property, if set. This will always
   * return a single directory, even though the spark.local.dir property might be a list of
   * multiple paths.
   */
  def getLocalDir(conf: SparkConf): String = {
    conf.get("spark.local.dir",  System.getProperty("java.io.tmpdir")).split(',')(0)
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
               !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String,  Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    val indx: Int = hostPort.lastIndexOf(':')
    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ...
    // but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    if (-1 == indx) {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      return retval
    }

    val retval = (hostPort.substring(0, indx).trim(), hostPort.substring(indx + 1).trim().toInt)
    hostPortParseResults.putIfAbsent(hostPort, retval)
    hostPortParseResults.get(hostPort)
  }

  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = daemonThreadFactoryBuilder.setNameFormat(prefix + "-%d").build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = daemonThreadFactoryBuilder.setNameFormat(prefix + "-%d").build()
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  private def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) {
      throw new IOException("Failed to list files for dir: " + file)
    }
    files
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   */
  def deleteRecursively(file: File) {
    if (file != null) {
      if ((file.isDirectory) && !isSymlink(file)) {
        for (child <- listFilesSafely(file)) {
          deleteRecursively(child)
        }
      }
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) {
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
        }
      }
    }
  }

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(dir: TachyonFile, client: TachyonFS) {
    if (!client.delete(dir.getPath(), true)) {
      throw new IOException("Failed to delete the tachyon dir: " + dir)
    }
  }

  /**
   * Check to see if file is a symbolic link.
   */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (isWindows) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    if (fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())) {
      return false
    } else {
      return true
    }
  }

  /**
   * Finds all the files in a directory whose last modified time is older than cutoff seconds.
   * @param dir  must be the path to a directory, or IllegalArgumentException is thrown
   * @param cutoff measured in seconds. Files older than this are returned.
   */
  def findOldFiles(dir: File, cutoff: Long): Seq[File] = {
    val currentTimeMillis = System.currentTimeMillis
    if (dir.isDirectory) {
      val files = listFilesSafely(dir)
      files.filter { file => file.lastModified < (currentTimeMillis - cutoff * 1000) }
    } else {
      throw new IllegalArgumentException(dir + " is not a directory!")
    }
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in megabytes to a human-readable string such as "4.0 MB".
   */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command in the given working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String], workingDir: File) {
    val process = new ProcessBuilder(command: _*)
        .directory(workingDir)
        .redirectErrorStream(true)
        .start()
    new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
  }

  /**
   * Execute a command in the current working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String]) {
    execute(command, new File("."))
  }

  /**
   * Execute a command and get its output, throwing an exception if it yields a code other than 0.
   */
  def executeAndGetOutput(command: Seq[String], workingDir: File = new File("."),
                          extraEnvironment: Map[String, String] = Map.empty): String = {
    val builder = new ProcessBuilder(command: _*)
        .directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    new Thread("read stderr for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val output = new StringBuffer
    val stdoutThread = new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          output.append(line)
        }
      }
    }
    stdoutThread.start()
    val exitCode = process.waitFor()
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
    output.toString
  }

  /**
   * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
   * default UncaughtExceptionHandler
   */
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case t: Throwable => ExecutorUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * A regular expression to match classes of the "core" Spark API that we want to skip when
   * finding the call site of a method.
   */
  private val SPARK_CLASS_REGEX = """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?\.[A-Z]""".r

  private[spark] class CallSiteInfo(val lastSparkMethod: String, val firstUserFile: String,
                                    val firstUserLine: Int, val firstUserClass: String) {

    /** Returns a printable version of the call site info suitable for logs. */
    override def toString = {
      "%s at %s:%s".format(lastSparkMethod, firstUserFile, firstUserLine)
    }
  }

  /**
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   */
  def getCallSiteInfo: CallSiteInfo = {
    val trace = Thread.currentThread.getStackTrace()
      .filterNot(_.getMethodName.contains("getStackTrace"))

    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var finished = false
    var firstUserClass = "<unknown>"

    for (el <- trace) {
      if (!finished) {
        if (SPARK_CLASS_REGEX.findFirstIn(el.getClassName).isDefined) {
          lastSparkMethod = if (el.getMethodName == "<init>") {
            // Spark method is a constructor; get its class name
            el.getClassName.substring(el.getClassName.lastIndexOf('.') + 1)
          } else {
            el.getMethodName
          }
        } else {
          firstUserLine = el.getLineNumber
          firstUserFile = el.getFileName
          firstUserClass = el.getClassName
          finished = true
        }
      }
    }
    new CallSiteInfo(lastSparkMethod, firstUserFile, firstUserLine, firstUserClass)
  }

  /** Return a string containing part of a file from byte 'start' to 'end'. */
  def offsetBytes(path: String, start: Long, end: Long): String = {
    val file = new File(path)
    val length = file.length()
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = new FileInputStream(file)

    stream.skip(effectiveStart)
    stream.read(buff)
    stream.close()
    Source.fromBytes(buff).mkString
  }

  /**
   * Clone an object using a Spark serializer.
   */
  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
   *
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  def isSpace(c: Char): Boolean = {
    " \t\r\n".indexOf(c) != -1
  }

  /**
   * Split a string of potentially quoted arguments from the command line the way that a shell
   * would do it to determine arguments to a command. For example, if the string is 'a "b c" d',
   * then it would be parsed as three arguments: 'a', 'b c' and 'd'.
   */
  def splitCommandString(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord() {
      buf += curWord.toString
      curWord.clear()
    }
    var i = 0
    while (i < s.length) {
      val nextChar = s.charAt(i)
      if (inDoubleQuote) {
        if (nextChar == '"') {
          inDoubleQuote = false
        } else if (nextChar == '\\') {
          if (i < s.length - 1) {
            // Append the next character directly, because only " and \ may be escaped in
            // double quotes after the shell's own expansion
            curWord.append(s.charAt(i + 1))
            i += 1
          }
        } else {
          curWord.append(nextChar)
        }
      } else if (inSingleQuote) {
        if (nextChar == '\'') {
          inSingleQuote = false
        } else {
          curWord.append(nextChar)
        }
        // Backslashes are not treated specially in single quotes
      } else if (nextChar == '"') {
        inWord = true
        inDoubleQuote = true
      } else if (nextChar == '\'') {
        inWord = true
        inSingleQuote = true
      } else if (!isSpace(nextChar)) {
        curWord.append(nextChar)
        inWord = true
      } else if (inWord && isSpace(nextChar)) {
        endWord()
        inWord = false
      }
      i += 1
    }
    if (inWord || inDoubleQuote || inSingleQuote) {
      endWord()
    }
    buf
  }

 /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // Handles idiosyncracies with hash (add more as required)
  def nonNegativeHash(obj: AnyRef): Int = {

    // Required ?
    if (obj eq null) return 0

    val hash = obj.hashCode
    // math.abs fails for Int.MinValue
    val hashAbs = if (Int.MinValue != hash) math.abs(hash) else 0

    // Nothing else to guard against ?
    hashAbs
  }

  /** Returns a copy of the system properties that is thread-safe to iterator over. */
  def getSystemProperties(): Map[String, String] = {
    System.getProperties.clone().asInstanceOf[java.util.Properties].toMap[String, String]
  }

  /**
   * Method executed for repeating a task for side effects.
   * Unlike a for comprehension, it permits JVM JIT optimization
   */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
   * Timing method based on iterations that permit JVM JIT optimization.
   * @param numIters number of iterations
   * @param f function to be executed
   */
  def timeIt(numIters: Int)(f: => Unit): Long = {
    val start = System.currentTimeMillis
    times(numIters)(f)
    System.currentTimeMillis - start
  }

  /**
   * Counts the number of elements of an iterator using a while loop rather than calling
   * [[scala.collection.Iterator#size]] because it uses a for loop, which is slightly slower
   * in the current version of Scala.
   */
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  /**
   * Creates a symlink. Note jdk1.7 has Files.createSymbolicLink but not used here
   * for jdk1.6 support.  Supports windows by doing copy, everything else uses "ln -sf".
   * @param src absolute path to the source
   * @param dst relative path for the destination
   */
  def symlink(src: File, dst: File) {
    if (!src.isAbsolute()) {
      throw new IOException("Source must be absolute")
    }
    if (dst.isAbsolute()) {
      throw new IOException("Destination must be relative")
    }
    var cmdSuffix = ""
    val linkCmd = if (isWindows) {
      // refer to http://technet.microsoft.com/en-us/library/cc771254.aspx
      cmdSuffix = " /s /e /k /h /y /i"
      "cmd /c xcopy "
    } else {
      cmdSuffix = ""
      "ln -sf "
    }
    import scala.sys.process._
    (linkCmd + src.getAbsolutePath() + " " + dst.getPath() + cmdSuffix) lines_!
      ProcessLogger(line => (logInfo(line)))
  }


  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef) = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /** Return an option that translates JNothing to None */
  def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  /** Return an empty JSON object */
  def emptyJson = JObject(List[JField]())

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
   */
  def getHadoopFileSystem(path: URI): FileSystem = {
    FileSystem.get(path, SparkHadoopUtil.get.newConfiguration())
  }

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
   */
  def getHadoopFileSystem(path: String): FileSystem = {
    getHadoopFileSystem(new URI(path))
  }

  /**
   * Return the absolute path of a file in the given directory.
   */
  def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
  }

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * Strip the directory from a path name
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * Wait for a process to terminate for at most the specified duration.
   * Return whether the process actually terminated after the given timeout.
   */
  def waitForProcess(process: Process, timeoutMs: Long): Boolean = {
    var terminated = false
    val startTime = System.currentTimeMillis
    while (!terminated) {
      try {
        process.exitValue
        terminated = true
      } catch {
        case e: IllegalThreadStateException =>
          // Process not terminated yet
          if (System.currentTimeMillis - startTime > timeoutMs) {
            return false
          }
          Thread.sleep(100)
      }
    }
    true
  }

  /**
   * Return the stderr of a process after waiting for the process to terminate.
   * If the process does not terminate within the specified timeout, return None.
   */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = Utils.waitForProcess(process, timeoutMs)
    if (terminated) {
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    } else {
      None
    }
  }

  /** 
   * Execute the given block, logging and re-throwing any uncaught exception.
   * This is particularly useful for wrapping code that runs in a thread, to ensure
   * that exceptions are printed, and to avoid having to catch Throwable.
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /** Returns true if the given exception was fatal. See docs for scala.util.control.NonFatal. */
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
        false
      case _ =>
        true
    }
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash, but this is inconsistent with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path

    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }
    uri.getScheme match {
      case windowsDrive(d) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case null =>
        // Preserve fragments for HDFS file name substitution (denoted by "#")
        // For instance, in "abc.py#xyz.py", "xyz.py" is the name observed by the application
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case _ =>
        uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val formattedPath = if (windows) formatWindowsPath(p) else p
        new URI(formattedPath).getScheme match {
          case windowsDrive(d) if windows => false
          case "local" | "file" | null => false
          case _ => true
        }
      }
    }
  }

}
