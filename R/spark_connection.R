


#' Get the SparkContext associated with a connection
#'
#' Get the SparkContext \code{spark_jobj} associated with a
#' \code{spark_connection}
#'
#' @param sc Connection to get SparkContext from
#'
#' @return Reference to SparkContext
#' @export
spark_context <- function(sc) {
  sc$spark_context
}

#' Get the JavaSparkContext associated with a connection
#'
#' Get the JavaSparkContext \code{spark_jobj} associated with a
#' \code{spark_connection}
#'
#' @param sc Connection to get SparkContext from
#'
#' @return Reference to SparkContext
#' @export
java_context <- function(sc) {
  sc$java_context
}

#' Get the HiveContext associated with a connection
#'
#' Get the HiveContext \code{spark_jobj} associated with a
#' \code{spark_connection}
#'
#' @param sc Connection to get HiveContext from
#'
#' @return Reference to HiveContext
#' @export
hive_context <- function(sc) {
  sc$hive_context
}


#' Get the spark_connection associated with an object
#'
#' S3 method to get the spark_connection associated with objects of
#' various types.
#'
#' @param x Object to extract connection from
#' @param ... Reserved for future use
#' @return A \code{spark_connection} object that can be passed to
#'   \code{\link{invoke_new}} and \code{\link{invoke_static}}.
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


#' Retrieves entries from the Spark log
#'
#' @param sc \code{spark_connection}
#' @param n Max number of log entries to retrieve (pass NULL to retrieve
#'   all lines of the log)
#' @param ... Unused (reserved for future use)
#'
#' @return Character vector with last \code{n} lines of the Spark log
#'   or for \code{spark_log_file} the full path to the log file.
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
  UseMethod("spark_web")
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







