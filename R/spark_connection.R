#' Access the Spark API
#'
#' Access the commonly-used Spark objects associated with a Spark instance.
#' These objects provide access to different facets of the Spark API.
#'
#' The \href{http://spark.apache.org/docs/latest/api/scala/#package}{Scala API documentation}
#' is useful for discovering what methods are available for each of the Spark
#' APIs.
#'
#' @section Hive Context:
#'
#' With Spark >= 2.0.0, the \code{HiveContext} class has been effectively
#' superceded by the \code{SparkSession} class, and so \code{hive_context}
#' will return a \code{SparkSession} object instead. (Note that the
#' \code{SparkSession} will have been constructed with Hive support when
#' available.)
#'
#' @section Spark Session:
#'
#' This object is only available since Spark 2.0.0, and provides an interface
#' that unifies the Spark SQL context + Hive context into a single object, and
#' its use is recommended over the older APIs for code targetting Spark 2.0.0
#' and above.
#'
#' @param sc A \code{spark_connection}.
#'
#' @name spark-api
NULL

#' @name spark-api
#' @export
spark_context <- function(sc) {
  sc$spark_context
}

#' @name spark-api
#' @export
java_context <- function(sc) {
  sc$java_context
}

#' @name spark-api
#' @export
hive_context <- function(sc) {
  sc$hive_context
}

#' @name spark-api
#' @export
spark_session <- function(sc) {
  sc$hive_context
}

#' Retrieve the Spark Connection Associated with an R Object
#'
#' Retrieve the \code{spark_connection} associated with an \R object.
#'
#' @param x An \R object from which a \code{spark_connection} can be obtained.
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_connection <- function(x, ...) {
  UseMethod("spark_connection")
}

#' @export
spark_connection.default <- function(x, ...) {
  stop("Unable to retreive a spark_connection from object of class ",
       paste(class(x), collapse = " "), call. = FALSE)
}

#' @export
spark_connection.spark_connection <- function(x, ...) {
  x
}

#' @export
spark_connection.spark_jobj <- function(x, ...) {
  x$connection
}

#' Check whether the connection is open
#'
#' @param sc \code{spark_connection}
#'
#' @keywords internal
#'
#' @export
connection_is_open <- function(sc) {
  UseMethod("connection_is_open")
}

#' Read configuration values for a connection
#'
#' @param sc \code{spark_connection}
#' @param prefix Prefix to read parameters for
#'   (e.g. \code{spark.context.}, \code{spark.sql.}, etc.)
#' @param not_prefix Prefix to not include.
#'
#' @return Named list of config parameters (note that if a prefix was
#'  specified then the names will not include the prefix)
#'
#' @export
connection_config <- function(sc, prefix, not_prefix = list()) {

  config <- sc$config
  master <- sc$master
  isLocal <- spark_master_is_local(master)

  configNames <- Filter(function(e) {
    found <- is.null(prefix) ||
      (substring(e, 1, nchar(prefix)) == prefix)

    if (grepl("\\.local$", e) && !isLocal)
      found <- FALSE

    if (grepl("\\.remote$", e) && isLocal)
      found <- FALSE

    found
  }, names(config))

  lapply(not_prefix, function(notPrefix) {
    configNames <<- Filter(function(e) {
      substring(e, 1, nchar(notPrefix)) != notPrefix
    }, configNames)
  })

  paramsNames <- lapply(configNames, function(configName) {
    paramName <- substr(configName, nchar(prefix) + 1, nchar(configName))
    paramName <- sub("(\\.local$)|(\\.remote$)", "", paramName, perl = TRUE)

    paramName
  })

  params <- lapply(configNames, function(configName) {
    config[[configName]]
  })

  names(params) <- paramsNames
  params
}

spark_master_is_local <- function(master) {
  grepl("^local(\\[[0-9\\*]*\\])?$", master, perl = TRUE)
}


#' View Entries in the Spark Log
#'
#' View the most recent entries in the Spark log. This can be useful when
#' inspecting output / errors produced by Spark during the invocation of
#' various commands.
#'
#' @param sc A \code{spark_connection}.
#' @param n The max number of log entries to retrieve. Use \code{NULL} to
#'   retrieve all entries within the log.
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_log <- function(sc, n = 100, ...) {
  UseMethod("spark_log")
}

#' @export
spark_log.default <- function(sc, n = 100, ...) {
  stop("Invalid class passed to spark_log")
}

#' @export
print.spark_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' Open the Spark web interface
#'
#' @inheritParams spark_log
#'
#' @export
spark_web <- function(sc, ...) {
  if (!is.null(sc$config$sparklyr.sparkui.url)) {
    structure(sc$config$sparklyr.sparkui.url, class = "spark_web_url")
  }
  else {
    UseMethod("spark_web")
  }
}

#' @export
spark_web.default <- function(sc, ...) {
  stop("Invalid class passed to spark_web")
}


#' @export
print.spark_web_url <- function(x, ...) {
  utils::browseURL(x)
}

initialize_connection <- function(sc) {

  # create the spark config
  conf <- invoke_new(sc, "org.apache.spark.SparkConf")
  conf <- invoke(conf, "setAppName", sc$app_name)
  conf <- invoke(conf, "setMaster", sc$master)
  conf <- invoke(conf, "setSparkHome", sc$spark_home)

  context_config <- connection_config(sc, "spark.", c("spark.sql."))
  apply_config(context_config, conf, "set", "spark.")

  # create the spark context and assign the connection to it
  sc$spark_context <- invoke_new(
    sc,
    "org.apache.spark.SparkContext",
    conf
  )
  sc$spark_context$connection <- sc

  # create the java spark context and assign the connection to it
  sc$java_context <- invoke_new(
    sc,
    "org.apache.spark.api.java.JavaSparkContext",
    sc$spark_context
  )
  sc$java_context$connection <- sc

  # create the hive context and assign the connection to it
  sc$hive_context <- create_hive_context(sc)
  sc$hive_context$connection <- sc

  # return the modified connection
  sc
}







