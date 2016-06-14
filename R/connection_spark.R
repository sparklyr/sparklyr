# register the spark_connection S3 class for use in setClass slots
methods::setOldClass("spark_connection")

spark_default_jars <- function() {
  jarsOption <- getOption("rspark.jars.default", NULL)

  if (is.null(jarsOption))
    system.file(file.path("java", "rspark_utils.jar"), package = "rspark")
  else
    jarsOption
}

#' Connects to Spark and establishes the Spark Context
#' @name spark_connect
#' @export
#' @param master Master definition to Spark cluster
#' @param app_name Application name to be used while running in the Spark cluster
#' @param version Version of the Spark cluster. Use spark_versions() for a list of supported Spark versions.
#' @param hadoop_version Version of Hadoop. Use spark_versions_hadoop() for a list of supported Hadoop versions.
#' @param cores Cores available for use for Spark. This option is only applicable to local installations. Use NULL
#' to prevent this package from making use of this parameter and "auto" to default to automatic core detection. Strictly
#' speaking, this option configures the number of available threads in a local spark instance; however, in practice, the
#' OS schedules one thread per core.
#' @param config A list containing configurations settings. This file overrides settings set on config.yml.
#' @examples
#' \dontrun{
#'  sc <- spark_connect(config = list(
#'    sql = list(
#'      spark.sql.shuffle.partitions = 1
#'    )
#'  ))
#' }
spark_connect <- function(master = "local",
                          app_name = "rspark",
                          version = NULL,
                          hadoop_version = NULL,
                          cores = "auto",
                          config = spark_config()) {
  sconFound <- spark_connection_find_scon(function(e) { e$master == master && e$appName == app_name })
  if (length(sconFound) == 1) {
    return(sconFound[[1]])
  }

  # verify that java is available
  if (!is_java_available()) {
    stop("Java is required to connect to Spark. Please download and install Java from ",
         java_install_url())
  }

  installInfo <- spark_install_find(version, hadoop_version, latest = FALSE)
  sparkVersion <- installInfo$sparkVersion
  hadoopVersion <- installInfo$hadoopVersion

  jars <- spark_default_jars()

  scon <- list(
    master = master,
    appName = app_name,
    sparkVersion = version,
    hadoopVersion = hadoop_version,
    cores = cores,
    isLocal = spark_master_is_local(master),
    reconnect = FALSE,
    installInfo = installInfo,
    config = config
  )
  scon <- structure(scon, class = "spark_connection")

  sconInst <- start_shell(scon, list(), jars)
  scon <- spark_connection_add_inst(scon$master, scon$appName, scon, sconInst)

  parentCall <- match.call()
  sconInst$connectCall <- paste("library(rspark)",
                                paste("sc <-", deparse(parentCall, width.cutoff = 500), collapse = " "),
                                sep = "\n")
  sconInst$onReconnect = list()

  reg.finalizer(baseenv(), function(x) {
    if (spark_connection_is_open(scon)) {
      stop_shell(scon)
    }
  }, onexit = TRUE)

  sconInst <- spark_connection_attach_context(scon, sconInst)
  sconInst$dbi <- NULL
  spark_connection_set_inst(scon, sconInst)

  on_connection_opened(scon, sconInst$connectCall)
  scon
}

spark_connection_attach_context <- function(scon, sconInst) {
  master <- scon$master

  cores <- scon$config[["sparklyr.cores"]]
  if (scon$master == "local" && !identical(cores, NULL))
    master <- paste("local[", cores, "]", sep = "")

  sconInst$sc <- spark_connection_create_context(scon, master, scon$appName, scon$installInfo$sparkVersionDir)
  if (identical(sconInst$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  sconInst
}

#' Disconnects from Spark and terminates the running application
#' @name spark_disconnect
#' @export
#' @param scon Spark connection provided by spark_connect
spark_disconnect <- function(scon) {
  stop_shell(scon)
}

#' Retrieves the last n entries in the Spark log
#' @name spark_log
#' @export
#' @param scon Spark connection provided by spark_connect
#' @param n Max number of log entries to retrieve
spark_log <- function(scon, n = 100) {
  log <- file(spark_log_file(scon))
  lines <- readLines(log)
  close(log)

  linesLog <- tail(lines, n = n)
  attr(linesLog, "class") <- "spark_log"

  linesLog
}

#' @rdname spark_log
#' @export
spark_log_file <- function(scon) {
  if (!spark_connection_is_open(scon)) {
    stop("The Spark conneciton is not open anymmore, log is not available")
  }
  sconInst <- spark_connection_get_inst(scon)
  sconInst$outputFile
}

#' Prints a spark_log object
#' @name spark_log
#' @export
#' @param x Spark connection provided by spark_connect
#' @param ... Additional parameters
print.spark_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' Opens the Spark web interface
#' @name spark_web
#' @export
#' @param scon Spark connection provided by spark_connect
spark_web <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)
  log <- file(sconInst$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  foundMatch <- FALSE
  uiLine <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine, perl=TRUE)
    match <-regmatches(uiLine, matches)
    if (length(match) > 0) {
      return(structure(match, class = "spark_web_url"))
    }
  }

  warning("Spark UI URL not found in logs, attempting to guess.")
  structure("http://localhost:4040", class = "spark_web_url")
}

#' @export
print.spark_web_url <- function(x, ...) {
  utils::browseURL(x)
}

spark_attach_connection <- function(object, scon) {
  if (inherits(object, "jobj")) {
    object$scon <- scon
  }
  else if (is.list(object) || inherits(object, "struct")) {
    object <- lapply(object, function(e) {
      spark_attach_connection(e, scon)
    })
  }
  else if (is.environment(object)) {
    object <- eapply(object, function(e) {
      spark_attach_connection(e, scon)
    })
  }

  object
}

spark_invoke_method <- function(scon, isStatic, objName, methodName, ...)
{
  # Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
  #
  #   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
  #
  if (is.null(scon)) {
    stop("The connection is no longer valid. Recreate using spark_connect.")
  }

  spark_reconnect_if_needed(scon)

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

  sconInst <- spark_connection_get_inst(scon)

  backend <- sconInst$backend
  writeBin(con, backend)
  returnStatus <- readInt(backend)

  if (returnStatus != 0)
    spark_report_invoke_error(scon, backend)

  object <- readObject(backend)
  spark_attach_connection(object, scon)
}

spark_report_invoke_error <- function(scon, backend) {

  # get error message from backend and report to R
  msg <- readString(backend)
  if (nzchar(msg))
    stop(msg, call. = FALSE)

  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- spark_log(scon)
    splat <- strsplit(log, "\\s+", perl = TRUE)
    n <- length(splat)
    timestamp <- splat[[n]][[2]]
    regex <- paste("\\b", timestamp, "\\b", sep = "")
    entries <- grep(regex, log, perl = TRUE, value = TRUE)
    pasted <- paste(entries, collapse = "\n")
    msg <- paste("failed to invoke spark command", pasted, sep = "\n")
  })

  stop(msg, call. = FALSE)
}

#' Executes a method on the given object
#' @name spark_invoke
#' @export
#' @param jobj Reference to a jobj retrieved using spark_invoke.
#'   Can alternately be a Spark connection, in this case it is
#'   converted to the Spark context jobj via the
#'   \code{\link{spark_context}} function.
#' @param methodName Name of class method to execute
#' @param ... Additional parameters that method requires
spark_invoke <- function (jobj, methodName, ...)
{
  if (inherits(jobj, "spark_connection"))
    jobj <- spark_context(jobj)

  spark_invoke_method(jobj$scon, FALSE, jobj$id, methodName, ...)
}

#' Executes an static method on the given object
#' @name spark_invoke_static
#' @export
#' @param scon Spark connection provided by spark_connect
#' @param objName Fully-qualified name to static class
#' @param methodName Name of class method to execute
#' @param ... Additional parameters that method requires
spark_invoke_static <- function (scon, objName, methodName, ...)
{
  spark_invoke_method(scon, TRUE, objName, methodName, ...)
}

#' Executes an static method on the given object
#' @name spark_invoke_static_ctor
#' @export
#' @param scon Spark connection provided by spark_connect
#' @param objName Fully-qualified name to static class
#' @param ... Additional parameters that method requires
spark_invoke_static_ctor <- function(scon, objName, ...)
{
  spark_invoke_method(scon, TRUE, objName, "<init>", ...)
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

  conf <- spark_invoke_static_ctor(scon, "org.apache.spark.SparkConf")
  conf <- spark_invoke(conf, "setAppName", appName)
  conf <- spark_invoke(conf, "setMaster", master)
  conf <- spark_invoke(conf, "setSparkHome", sparkHome)

  params <- spark_config_params(scon$config, "spark.context.")
  lapply(names(params), function(paramName) {
    conf <<- spark_invoke(conf, "set", paramName, params[[paramName]])
  })

  spark_invoke_static_ctor(
    scon,
    "org.apache.spark.SparkContext",
    conf
  )
}

#' Retrieves the SparkContext reference from a Spark Connection
#' @name spark_context
#' @export
#' @param scon Spark connection provided by spark_connect
spark_context <- function(scon) {
  spark_reconnect_if_needed(scon)

  sconInst <- spark_connection_get_inst(scon)
  sconInst$sc
}

#' Retrieves master from a Spark Connection
#' @name spark_context_master
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_master <- function(scon) {
  scon$master
}

#' Retrieves the application name from a Spark Connection
#' @name spark_connection_app_name
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_app_name <- function(scon) {
  scon$appName
}

#' TRUE if the Spark Connection is a local install
#' @name spark_connection_is_local
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_is_local <- function(scon) {
  scon$isLocal
}

spark_master_is_local <- function(master) {
  grepl("^local(\\[[0-9\\*]*\\])?$", master, perl = TRUE)
}

#' Number of cores available in the local install
#' @name spark_connection_local_cores
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_local_cores <- function(scon) {
  scon$cores
}

#' Checks to see if the connection into Spark is still open
#' @name spark_connection_is_open
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_is_open <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)

  bothOpen <- FALSE
  if (!identical(sconInst, NULL)) {
    backend <- sconInst$backend
    monitor <- sconInst$monitor

    tryCatch({
      bothOpen <- isOpen(backend) && isOpen(monitor)
    }, error = function(e) {
    })
  }

  bothOpen
}

#' Closes all existing connections. Returns the total of connections closed.
#' @name spark_disconnect_all
#' @export
spark_disconnect_all <- function() {
  scons <- spark_connection_find_scon(function(e) {
    spark_connection_is_open(e)
  })

  length(lapply(scons, function(e) {
    spark_disconnect(e)
  }))
}
