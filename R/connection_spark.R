# List of connections
.rspark.connections <- new.env(parent = emptyenv())

spark_connections_close_all <- function(x) {
  lapply(.rspark.connections, function(scon) {
    if (spark_connection_is_open(scon$id)) {
      stop_shell(scon$id)
    }
    NULL
  })
}

reg.finalizer(baseenv(), spark_connections_close_all, onexit = TRUE)

spark_connect_with_shell <- function(scon) {
  installInfo = spark_install_info(scon$version)
  scon <- start_shell(scon, installInfo)

  scon$id <- length(.rspark.connections)
  .rspark.connections[[scon$id]] <- scon
  sconref <- scon$id
  attr(sconref, "class") <- "spark_connection"

  scon$sc <- spark_connection_create_context(sconref, scon$master, scon$appName, installInfo$sparkVersionDir)
  if (identical(scon$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  scon$useHive <- compareVersion(scon$version, "2.0.0") < 0
  scon$isLocal <- grepl("^local(\\[[0-9\\*]*\\])$", scon$master, perl = TRUE)

  sconref
}

#' Connects to Spark and establishes the Spark Context
#' @name spark_connect
#' @export
#' @param master Master definition to Spark cluster
#' @param appName Application name to be used while running in the Spark cluster
#' @param version Version of the Spark cluster
#' @param cores Number of cores available in the cluster. This if often usefull to optimize other parameters,
#' for instance, to fine-tune the number of partitions to use when shuffling data. Use NULL to use default values
#' or 0 to avoid any optimizations.
#' @param reconnect Allows the Spark connection to reconnect when the connection is lost
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
    reconnect = reconnect
  )

  sconref <- spark_connect_with_shell(scon)

  sconref
}

#' Disconnects from Spark and terminates the running application
#' @name spark_disconnect
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_disconnect <- function(sconref) {
  stop_shell(sconref)
}

#' Retrieves the last n entries in the Spark log
#' @name spark_log
#' @export
#' @param sconref Spark connection reference provided by spark_connect
#' @param n Max number of log entries to retrieve
spark_log <- function(sconref, n = 100) {
  scon <- spark_connection_from_ref(sconref)

  log <- file(scon$outputFile)
  lines <- readLines(log)
  close(log)

  linesLog <- tail(lines, n = n)
  attr(linesLog, "class") <- "spark_log"

  linesLog
}

#' Prints a spark_log object
#' @name spark_log
#' @export
#' @param x Spark log provided by spark_log
#' @param ... Additional parameters
print.spark_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' Opens the Spark web interface
#' @name spark_web
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_web <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)

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

spark_attach_connection <- function(object, sconref) {
  if ("jobj" %in% attr(object, "class")) {
    object$sconref <- sconref
  }
  else if (is.list(object) || "struct" %in% attr(object, "class")) {
    object <- lapply(object, function(e) {
      spark_attach_connection(e, sconref)
    })
  }
  else if (is.vector(object) && length(object) > 1) {
    object <- vapply(object, function(e) {
      spark_attach_connection(e, sconref)
    }, "")
  }
  else if (is.environment(object)) {
    object <- eapply(object, function(e) {
      spark_attach_connection(e, sconref)
    })
  }

  object
}

spark_invoke_method <- function (sconref, isStatic, objName, methodName, ...)
{
  # Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
  #
  #   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
  #
  scon <- spark_connection_from_ref(sconref)

  if (!spark_connection_is_open(scon) && scon$reconnect) {
    spark_connect_with_shell(scon)
  }

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

  object <- readObject(scon$backend)
  spark_attach_connection(object, sconref)
}

#' Executes a method on the given object
#' @name spark_invoke
#' @export
#' @param jobj Reference to a jobj retrieved using spark_invoke
#' @param methodName Name of class method to execute
#' @param ... Additional parameters that method requires
spark_invoke <- function (jobj, methodName, ...)
{
  spark_invoke_method(jobj$sconref, FALSE, jobj$id, methodName, ...)
}

#' Executes an static method on the given object
#' @name spark_invoke_static
#' @export
#' @param sconref Spark connection reference provided by spark_connect
#' @param objName Fully-qualified name to static class
#' @param methodName Name of class method to execute
#' @param ... Additional parameters that method requires
spark_invoke_static <- function (sconref, objName, methodName, ...)
{
  spark_invoke_method(sconref, TRUE, objName, methodName, ...)
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
spark_connection_create_context <- function(sconref, master, appName, sparkHome) {
  sparkHome <- as.character(normalizePath(sparkHome, mustWork = FALSE))

  spark_invoke_static(
    sconref,

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
#' @param sconef Spark connection reference provided by spark_connect
spark_context <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  scon$sc
}

#' Retrieves master from a Spark Connection
#' @name spark_context_master
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_connection_master <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  scon$master
}

#' Retrieves the application name from a Spark Connection
#' @name spark_connection_app_name
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_connection_app_name <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  scon$appName
}

#' TRUE if the Spark Connection is a local install
#' @name spark_connection_is_local
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_connection_is_local <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  scon$isLocal
}

#' Number of cores available in the cluster
#' @name spark_connection_cores
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_connection_cores <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  scon$cores
}

#' Checks to see if the connection into Spark is still open
#' @name spark_connection_is_open
#' @export
#' @param sconref Spark connection reference provided by spark_connect
spark_connection_is_open <- function(sconref) {
  scon <- spark_connection_from_ref(sconref)
  isOpen(scon$backend) && isOpen(scon$monitor)
}

spark_connection_from_ref <- function(sconref) {
  if (!(sconref %in% .rspark.connections)) {
    stop("Invalid Spark connection reference")
  }

  .rspark.connections[[sconref]]
}
