#' Provides support to download and install the given Spark version
#' @export
#' @import rappdirs
#' @rdname spark-connection
spark_install <- function(version = "1.6.0") {
  componentName <- paste("spark-", version, "-bin-hadoop2.6", sep = "")

  packageName <- paste(componentName, ".tgz", sep = "")
  packageSource <- "http://d3kbcqa49mib13.cloudfront.net"

  sparkDir <- file.path(getwd(), "spark")
  if (is.installed("rappdirs")) {
    sparkDir <- rappdirs::app_dir("spark", "rstudio")$cache()
  }

  if (!dir.exists(sparkDir)) {
    print("Local spark directory for this project not found, creating.")
    dir.create(sparkDir)
  }

  packagePath <- file.path(sparkDir, packageName)

  if (!file.exists(packagePath)) {
    print("Spark package not found, downloading.")
    download.file(file.path(packageSource, packageName), destfile = packagePath)
  }

  sparkVersionDir <- file.path(sparkDir, componentName)

  if (!dir.exists(sparkVersionDir)) {
    untar(tarfile = packagePath, exdir = sparkDir)
  }

  list (
    sparkDir = sparkDir,
    sparkVersionDir = sparkVersionDir
  )
}

spark_connect_with_shell <- function(master, appName, installInfo) {
  scon <- start_shell(installInfo)

  scon$sc <- spark_connection_create_context(scon, master, appName, installInfo$sparkVersionDir)
  if (identical(scon$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  scon$master <- master
  scon$appName <- appName

  scon
}

#' Connects to Spark and establishes the Spark Context
#' @export
#' @rdname spark-connection
spark_connect <- function(master = "local", appName = "rspark", version = "1.6.0", installInfo = spark_install()) {
  spark_connect_with_shell(master = master,
                           appName = appName,
                           installInfo = installInfo)
}

#' Disconnects from Spark and terminates the running application
#' @export
#' @rdname spark-connection
spark_disconnect <- function(sc) {
  stop_shell(sc)
}

#' Prints the last n entries in the Spark log
#' @export
#' @rdname spark-connection
spark_log <- function(con, n = 100) {
  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- tail(lines, n = n)

  paste(lines, collapse = "\n")
}

#' Opens the Spark web interface
#' @export
#' @rdname spark-connection
spark_web <- function(con) {
  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  ui_line <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(ui_line) > 0) {
    matches <- regexpr("http://.*", ui_line, perl=TRUE)
    match <-regmatches(ui_line, matches)
    if (length(match) > 0) {
      browseURL(match)
    }
  }
}

spark_invoke_method <- function (scon, isStatic, objName, methodName, ...)
{
  # Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
  #
  #   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
  #

  rc <- rawConnection(raw(), "r+")
  writeBoolean(rc, isStatic)
  writeString(rc, objName)
  writeString(rc, methodName)

  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)

  writeBin(con, scon$backend)
  returnStatus <- readInt(scon$backend)

  if (returnStatus != 0) {
    stop(readString(scon$backend))
  }

  readObject(scon$backend)
}

#' Executes a method on the given object
#' @export
#' @rdname spark-connection
spark_invoke <- function (scon, obj, methodName, ...)
{
  spark_invoke_method(scon, FALSE, obj$id, methodName, ...)
}

#' Executes an static method on the given object
#' @export
#' @rdname spark-connection
spark_invoke_static <- function (scon, objName, methodName, ...)
{
  spark_invoke_method(scon, TRUE, objName, methodName, ...)
}

# API into https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
#
# def createSparkContext(
#   master: String,                               // The Spark master URL.
#   appName: String,                              // Application name to register with cluster manager
#   sparkHome: String,                            // Spark Home directory
#   jars: Array[String],                          // Character string vector of jar files to pass to the worker nodes.
#   sparkEnvirMap: JMap[Object, Object],          // Named list of environment variables to set on worker nodes.
#   sparkExecutorEnvMap: JMap[Object, Object])    // Named list of environment variables to be used when launching executors.
#   : JavaSparkContext
#
spark_connection_create_context <- function(scon, master, appName, sparkHome) {
  sparkHome <- as.character(normalizePath(sparkHome, mustWork = FALSE))

  spark_invoke_static(
    scon,

    "org.apache.spark.api.r.RRDD",
    "createSparkContext",

    master,
    appName,
    sparkHome,
    list(),
    new.env(),
    new.env()
  )
}

#' Retrieves the SparkContext reference from a Spark Connection
#' @export
#' @rdname spark-connection
spark_context <- function(scon) {
  scon$sc
}
