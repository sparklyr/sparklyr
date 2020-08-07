#' Retrieve a Spark DataFrame
#'
#' This S3 generic is used to access a Spark DataFrame object (as a Java
#' object reference) from an \R object.
#'
#' @param x An \R object wrapping, or containing, a Spark DataFrame.
#' @param ... Optional arguments; currently unused.
#' @return A \code{\link{spark_jobj}} representing a Java object reference
#'   to a Spark DataFrame.
#'
#' @export
spark_dataframe <- function(x, ...) {
  UseMethod("spark_dataframe")
}

#' @export
spark_dataframe.default <- function(x, ...) {
  stop("Unable to retrieve a Spark DataFrame from object of class ",
    paste(class(x), collapse = " "),
    call. = FALSE
  )
}

#' @export
spark_dataframe.spark_jobj <- function(x, ...) {
  x
}
