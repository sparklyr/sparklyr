#' Access the Spark API
#'
#' Access the commonly-used Spark objects associated with a Spark instance.
#' These objects provide access to different facets of the Spark API.
#'
#' The \href{http://spark.apache.org/docs/latest/api/scala/#package}{Scala API documentation}
#' is useful for discovering what methods are available for each of these
#' objects. Use \code{\link{invoke}} to call methods on these objects.
#'
#' @section Spark Context:
#'
#' The main entry point for Spark functionality. The \strong{Spark Context}
#' represents the connection to a Spark cluster, and can be used to create
#' \code{RDD}s, accumulators and broadcast variables on that cluster.
#'
#' @section Java Spark Context:
#'
#' A Java-friendly version of the aforementioned \strong{Spark Context}.
#'
#' @section Hive Context:
#'
#' An instance of the Spark SQL execution engine that integrates with data
#' stored in Hive. Configuration for Hive is read from \code{hive-site.xml} on
#' the classpath.
#'
#' Starting with Spark >= 2.0.0, the \strong{Hive Context} class has been
#' deprecated -- it is superceded by the \strong{Spark Session} class, and
#' \code{hive_context} will return a \strong{Spark Session} object instead.
#' Note that both classes share a SQL interface, and therefore one can invoke
#' SQL through these objects.
#'
#' @section Spark Session:
#'
#' Available since Spark 2.0.0, the \strong{Spark Session} unifies the
#' \strong{Spark Context} and \strong{Hive Context} classes into a single
#' interface. Its use is recommended over the older APIs for code
#' targeting Spark 2.0.0 and above.
#'
#' @param sc A \code{spark_connection}.
#'
#' @name spark-api
NULL

#' @name spark-api
#' @export
spark_context <- function(sc) {
  sc$state$spark_context
}

#' @name spark-api
#' @export
java_context <- function(sc) {
  sc$state$java_context
}

#' @name spark-api
#' @export
hive_context <- function(sc) {
  UseMethod("hive_context")
}

#' @name spark-api
#' @export
spark_session <- function(sc) {
  UseMethod("spark_session")
}


#' @export
hive_context.spark_connection <- function(sc) {
  sc$state$hive_context
}

#' @export
spark_session.spark_connection <- function(sc) {
  sc$state$hive_context
}

#' Retrieve the Spark Connection Associated with an R Object
#'
#' Retrieve the \code{spark_connection} associated with an \R object.
#'
#' @param x An \R object from which a \code{spark_connection} can be obtained.
#' @param ... Optional arguments; currently unused.
#'
#' @exportClass spark_connection
#' @export
spark_connection <- function(x, ...) {
  UseMethod("spark_connection")
}

#' @export
spark_connection.default <- function(x, ...) {

  stop("Unable to retrieve a spark_connection from object of class ",
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

    if (is.character(config[[e]]) && all(nchar(config[[e]]) == 0))
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

#' View Entries in the Spark Log
#'
#' View the most recent entries in the Spark log. This can be useful when
#' inspecting output / errors produced by Spark during the invocation of
#' various commands.
#'
#' @param sc A \code{spark_connection}.
#' @param n The max number of log entries to retrieve. Use \code{NULL} to
#'   retrieve all entries within the log.
#' @param filter Character string to filter log entries.
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_log <- function(sc, n = 100, filter = NULL, ...) {
  UseMethod("spark_log")
}

#' @export
spark_log.default <- function(sc, n = 100, ...) {
  stop("Invalid class passed to spark_log")
}

#' @export
print.spark_log <- function(x, ...) {
  cat(x, sep = "\n")
}

#' Open the Spark web interface
#'
#' @inheritParams spark_log
#'
#' @export
spark_web <- function(sc, ...) {
  if (!identical(sc$state, NULL) && !identical(sc$state$spark_web, NULL)) return(sc$state$spark_web)

  sparkui_url <- spark_config_value(
    sc$config, c("sparklyr.web.spark", "sparklyr.sparkui.url")
  )

  if (!is.null(sparkui_url)) {
    structure(sparkui_url, class = "spark_web_url")
  }
  else if (spark_version(sc) >= "2.0.0" &&
           !spark_context(sc) %>% invoke("uiWebUrl") %>% invoke("isEmpty")) {

    spark_context(sc) %>%
      invoke("uiWebUrl") %>%
      invoke("get") %>%
      structure(class = "spark_web_url")
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
  UseMethod("initialize_connection")
}

new_spark_connection <- function(scon, ..., class = character()) {
  structure(
    scon,
    ...,
    class = c("spark_connection", class, "DBIConnection")
  )
}

new_spark_shell_connection <- function(scon, ..., class = character()) {
  new_spark_connection(
    scon,
    ...,
    class = c(class, "spark_shell_connection")
  )
}

new_spark_gateway_connection <- function(scon, ..., class = character()) {
  new_spark_shell_connection(
    scon,
    ...,
    class = c(class, "spark_gateway_connection")
  )
}

new_livy_connection <- function(scon) {
  new_spark_connection(
    scon,
    class = "livy_connection"
  )
}
