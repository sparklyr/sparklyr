#' Find the SPARK_HOME directory for a version of Spark
#'
#' Find the SPARK_HOME directory for a given version of Spark that
#' was previously installed using \code{\link{spark_install}}.
#'
#' @param version Version of Spark
#' @param hadoop_version Version of Hadoop
#'
#' @return Path to SPARK_HOME (or \code{NULL} if the specified version
#'   was not found).
#'
#' @keywords internal
#'
#' @export
spark_home_dir <- function(version = NULL, hadoop_version = NULL) {
  tryCatch({
    installInfo <- spark_install_find(
      version = version,
      hadoop_version = hadoop_version
    )

    installInfo$sparkVersionDir
  },
    error = function(e) {
      NULL
    })
}


#' Set the SPARK_HOME environment variable
#'
#' Set the \code{SPARK_HOME} environment variable. This slightly speeds up some
#' operations, including the connection time.
#'
#' @param path A string containing the path to the installation location of
#' Spark. If \code{NULL}, the path to the most latest Spark/Hadoop versions is
#' used.
#' @param ... Additional parameters not currently used.
#'
#' @return The function is mostly invoked for the side-effect of setting the
#' \code{SPARK_HOME} environment variable. It also returns \code{TRUE} if the
#' environment was successfully set, and \code{FALSE} otherwise.
#'
#' @examples
#' \dontrun{
#' # Not run due to side-effects
#' spark_home_set()
#' }
#' @export
spark_home_set <- function(path = NULL, ...) {
  verbose <- spark_config_value(list(), "sparklyr.verbose", is.null(path))
  if(is.null(path)) {
    path <- spark_install_find()$sparkVersionDir
  }
  if(verbose) {
    message("Setting SPARK_HOME environment variable to ", path)
  }
  Sys.setenv(SPARK_HOME = path)
}
