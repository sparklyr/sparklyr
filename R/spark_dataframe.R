#' Get the Spark DataFrame associated with an object
#'
#' S3 method to get the Spark DataFrame associated with objects of
#' various types.
#'
#' @param x Object to get DataFrame from
#' @param ... Reserved for future use
#' @return Reference to DataFrame
#'
#' @export
spark_dataframe <- function(x, ...) {
  UseMethod("spark_dataframe")
}

#' @export
spark_dataframe.default <- function(x, ...) {
  stop("Unable to retreive a Spark DataFrame from object of class ",
       paste(class(x), collapse = " "), call. = FALSE)
}

#' @export
spark_dataframe.spark_jobj <- function(x, ...) {
  x
}

