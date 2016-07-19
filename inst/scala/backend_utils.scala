package sparklyr

import java.io._
import java.io.File
import java.util.Arrays

import org.apache.spark.{SparkEnv, SparkException}

object BackendUtils {
  // Local path where R binary packages built from R source code contained in the spark
  // packages specified with "--packages" or "--jars" command line option reside.
  var rPackages: Option[String] = None

  /**
    * Get the SparkR package path in the local spark distribution.
  */
  def localSparkRPackagePath: Option[String] = {
    val sparkHome = sys.env.get("SPARK_HOME").orElse(sys.props.get("spark.test.home"))
    sparkHome.map(
      Seq(_, "R", "lib").mkString(File.separator)
    )
  }

  /**
    * Get the list of paths for R packages in various deployment modes, of which the first
  * path is for the SparkR package itself. The second path is for R packages built as
  * part of Spark Packages, if any exist. Spark Packages can be provided through the
  *  "--packages" or "--jars" command line options.
  *
    * This assumes that Spark properties `spark.master` and `spark.submit.deployMode`
  * and environment variable `SPARK_HOME` are set.
  */
  def sparkRPackagePath(isDriver: Boolean): Seq[String] = {
    val (master, deployMode) =
      if (isDriver) {
        (sys.props("spark.master"), sys.props("spark.submit.deployMode"))
      } else {
        val sparkConf = SparkEnv.get.conf
        (sparkConf.get("spark.master"), sparkConf.get("spark.submit.deployMode", "client"))
      }

    val isYarnCluster = master != null && master.contains("yarn") && deployMode == "cluster"
    val isYarnClient = master != null && master.contains("yarn") && deployMode == "client"

    // In YARN mode, the SparkR package is distributed as an archive symbolically
    // linked to the "sparkr" file in the current directory and additional R packages
    // are distributed as an archive symbolically linked to the "rpkg" file in the
    // current directory.
    //
      // Note that this does not apply to the driver in client mode because it is run
    // outside of the cluster.
    if (isYarnCluster || (isYarnClient && !isDriver)) {
      val sparkRPkgPath = new File("sparkr").getAbsolutePath
      val rPkgPath = new File("rpkg")
      if (rPkgPath.exists()) {
        Seq(sparkRPkgPath, rPkgPath.getAbsolutePath)
      } else {
        Seq(sparkRPkgPath)
      }
    } else {
      // Otherwise, assume the package is local
      // TODO: support this for Mesos
      val sparkRPkgPath = localSparkRPackagePath.getOrElse {
        throw new SparkException("SPARK_HOME not set. Can't locate SparkR package.")
      }
      if (!rPackages.isEmpty) {
        Seq(sparkRPkgPath, rPackages.get)
      } else {
        Seq(sparkRPkgPath)
      }
    }
  }

  /** Check if R is installed before running tests that use R commands. */
  def isRInstalled: Boolean = {
    try {
      val builder = new ProcessBuilder(Arrays.asList("R", "--version"))
      builder.start().waitFor() == 0
    } catch {
      case e: Exception => false
    }
  }

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}
