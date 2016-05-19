# register the spark_connection S3 class for use in setClass slots
methods::setOldClass("spark_connection")

#' Connects to Spark and establishes the Spark Context
#' @name spark_connect
#' @export
#' @param master Master definition to Spark cluster
#' @param appName Application name to be used while running in the Spark cluster
#' @param version Version of the Spark cluster
#' @param cores Number of cores available in the cluster. This if often usefull to optimize other parameters,
#' for instance, to fine-tune the number of partitions to use when shuffling data. Use NULL to use default values
#' or 0 to avoid any optimizations.
#' @param reconnect Reconnects automatically to Spark on the next attempt to access an Spark resource
spark_connect <- function(master = "local[*]",
                          appName = "rspark",
                          version = "1.6.0",
                          cores = NULL,
                          reconnect = TRUE) {
  scon <- list(
    master = master,
    appName = appName,
    version = version,
    cores = cores,
    useHive = compareVersion(version, "2.0.0") < 0,
    isLocal = grepl("^local(\\[[0-9\\*]*\\])$", master, perl = TRUE),
    reconnect = reconnect,
    installInfo = spark_install_info(version)
  )

  sconInst <- start_shell(scon$installInfo)

  sconInst$onReconnect = list()
  scon$sconRef <- spark_connection_add_inst(sconInst)

  reg.finalizer(baseenv(), function(x) {
    if (spark_connection_is_open(scon)) {
      stop_shell(scon)
    }
  }, onexit = TRUE)

  spark_connection_attach_context(scon)

  connectCall <- deparse(match.call())

  on_connection_opened(scon, connectCall)

  structure(scon, class = "spark_connection")
}

spark_connection_attach_context <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)

  sconInst$sc <- spark_connection_create_context(scon, scon$master, scon$appName, scon$installInfo$sparkVersionDir)
  if (identical(sconInst$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  spark_connection_set_inst(scon, sconInst)
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
  spark_reconnect_if_needed(scon)

  if (!spark_connection_is_open(scon)) {
    stop("The Spark conneciton is not open anymmore, log is not available")
  }

  sconInst <- spark_connection_get_inst(scon)
  log <- file(sconInst$outputFile)
  lines <- readLines(log)
  close(log)

  linesLog <- tail(lines, n = n)
  attr(linesLog, "class") <- "spark_log"

  linesLog
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
  spark_reconnect_if_needed(scon)

  log <- file(scon$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  uiLine <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine, perl=TRUE)
    match <-regmatches(uiLine, matches)
    if (length(match) > 0) {
      browseURL(match)
    }
  }
}

spark_attach_connection <- function(object, scon) {
  if ("jobj" %in% attr(object, "class")) {
    object$scon <- scon
  }
  else if (is.list(object) || "struct" %in% attr(object, "class")) {
    object <- lapply(object, function(e) {
      spark_attach_connection(e, scon)
    })
  }
  else if (is.vector(object) && length(object) > 1) {
    object <- vapply(object, function(e) {
      spark_attach_connection(e, scon)
    }, "")
  }
  else if (is.environment(object)) {
    object <- eapply(object, function(e) {
      spark_attach_connection(e, scon)
    })
  }

  object
}

spark_reconnect_if_needed <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)
  if (!spark_connection_is_open(scon) && scon$reconnect == TRUE && (identical(sconInst, NULL) || sconInst$finalized == FALSE)) {
    installInfo <- spark_install_info(scon$version)
    sconInst <- start_shell(installInfo)

    spark_connection_set_inst(scon, sconInst)
    spark_connection_attach_context(scon)

    lapply(sconInst$onReconnects, function(onReconnect) {
      onReconnect()
    })
  }
}

spark_invoke_method <- function (scon, isStatic, objName, methodName, ...)
{
  # Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
  #
  #   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
  #
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

  if (returnStatus != 0) {
    stop(readString(backend))
  }

  object <- readObject(backend)
  spark_attach_connection(object, scon)
}

#' Executes a method on the given object
#' @name spark_invoke
#' @export
#' @param jobj Reference to a jobj retrieved using spark_invoke
#' @param methodName Name of class method to execute
#' @param ... Additional parameters that method requires
spark_invoke <- function (jobj, methodName, ...)
{
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

#' Number of cores available in the cluster
#' @name spark_connection_cores
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_cores <- function(scon) {
  scon$cores
}

#' Checks to see if the connection into Spark is still open
#' @name spark_connection_is_open
#' @export
#' @param scon Spark connection provided by spark_connect
spark_connection_is_open <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)

  backend <- sconInst$backend
  monitor <- sconInst$monitor

  bothOpen <- FALSE
  tryCatch({
    bothOpen <- isOpen(backend) && isOpen(monitor)
  }, error = function(e) {
  })

  bothOpen
}

# List of low-level connection references
.rspark.connection.instances <- new.env(parent = emptyenv())

spark_connection_get_inst <- function(scon) {
  sconRef <- scon$sconRef
  sconInst <- NULL
  if (sconRef %in% names(.rspark.connection.instances)) {
    sconInst <- .rspark.connection.instances[[sconRef]]
  }

  sconInst
}

spark_connection_add_inst <- function(sconInst) {
  sconRef <- as.character(length(.rspark.connection.instances) + 1)
  .rspark.connection.instances[[sconRef]] <- sconInst
  sconRef
}

spark_connection_set_inst <- function(scon, sconInst) {
  .rspark.connection.instances[[scon$sconRef]] <- sconInst
}

spark_connection_on_reconnect <- function(scon, onReconnect) {
  sconInst <- spark_connection_get_inst(scon)
  sconInst$onReconnect[[length(sconInst$onReconnect) + 1]] <- onReconnect
  spark_connection_set_inst(scon, sconInst)
}

