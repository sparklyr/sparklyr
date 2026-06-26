spark_require_version <- function(
  sc,
  required,
  module = NULL,
  required_max = NULL
) {
  # guess module based on calling function
  if (is.null(module)) {
    call <- sys.call(sys.parent())
    module <- tryCatch(as.character(call[[1]]), error = function(ex) "")
  }

  # check and report version requirements
  version <- spark_version(sc)
  if (version < required) {
    fmt <- "%s requires Spark %s or higher."
    msg <- sprintf(fmt, module, required, version)
    stop(msg, call. = FALSE)
  } else if (!is.null(required_max)) {
    if (version >= required_max) {
      fmt <- "%s is removed in Spark %s."
      msg <- sprintf(fmt, module, required_max, version)
      stop(msg, call. = FALSE)
    }
  }

  TRUE
}


is_required_spark <- function(x, required_version) {
  UseMethod("is_required_spark")
}


#' @export
is_required_spark.spark_connection <- function(x, required_version) {
  version <- spark_version(x)
  version >= required_version
}


#' @export
is_required_spark.spark_jobj <- function(x, required_version) {
  sc <- spark_connection(x)
  is_required_spark(sc, required_version)
}


spark_param_deprecated <- function(param, version = "3.x") {
  warning("The '", param, "' parameter is deprecated in Spark ", version)
}


package_version2 <- function(x) {
  if (inherits(x, "numeric_version")) {
    x <- as.character(x)
  }
  package_version(x)
}
