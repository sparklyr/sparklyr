#' Replace Missing Values in Objects
#'
#' This S3 generic provides an interface for replacing
#' \code{\link{NA}} values within an object.
#'
#' @param object An \R object.
#' @param ... Arguments passed along to implementing methods.
#'
#' @export
na.replace <- function(object, ...) {
  UseMethod("na.replace")
}

#' @export
na.replace.tbl_spark <- function(object, ...) {
  na.replace(spark_dataframe(object), ...)
}

#' @export
na.replace.spark_jobj <- function(object, ...) {
  dots <- list(...)
  enumerate(dots, function(key, val) {
    na <- invoke(object, "na")
    object <<- if (is.null(key))
      invoke(na, "fill", val)
    else
      invoke(na, "fill", val, as.list(key))
  })
  sdf_register(object)
}

#' @export
na.omit.tbl_spark <- function(object, columns = NULL, ...) {
  na.omit(spark_dataframe(object), columns = NULL, ...)
}

#' @export
na.omit.spark_jobj <- function(object, columns = NULL, ...) {

  # report number of rows dropped if requested
  verbose <- sparklyr_boolean_option(
    "sparklyr.na.omit.verbose",
    "sparklyr.na.action.verbose",
    "sparklyr.verbose"
  )

  n_before <- if (verbose) invoke(object, "count")
  dropped  <- sdf_na_omit(object, columns)
  n_after  <- if (verbose) invoke(dropped, "count")

  if (verbose) {
    n_diff <- n_before - n_after
    if (n_diff > 0) {
      fmt <- "* Dropped %s rows with 'na.omit' (%s => %s)"
      message(sprintf(fmt, n_diff, n_before, n_after))
    } else {
      message("* No rows dropped by 'na.omit' call")
    }
  }

  # using a df created from drop actions reduces performance, see #308.
  if (identical(n_before, n_after)) {
    object
  } else {
    result <- sdf_register(dropped)
    invoke(spark_dataframe(result), "cache")

    result
  }
}

#' @export
na.fail.tbl_spark <- function(object, columns = NULL, ...) {
  na.fail(spark_dataframe(object), ...)
}

#' @export
na.fail.spark_jobj <- function(object, columns = NULL, ...) {
  n_before <- invoke(object, "count")
  dropped  <- sdf_na_omit(object, columns)
  n_after  <- invoke(dropped, "count")

  if (n_before != n_after)
    stop("* missing values in object")

  object
}

# Spark DataFrame NA Routines ----

apply_na_action <- function(x, response = NULL, features = NULL, na.action) {

  # early exit for NULL, NA na.action
  if (is.null(na.action))
    return(x)

  # attempt to resolve character na.action
  if (is.character(na.action)) {
    if (!exists(na.action, envir = parent.frame(), mode = "function"))
      stop("no function with name '", na.action, "' found")

    na.action <- get(na.action, envir = parent.frame(), mode = "function")
  }

  if (!is.function(na.action))
    stop("'na.action' is not a function")

  # attempt to apply 'na.action'
  na.action(x,
            response = response,
            features = features,
            columns = c(response, features))
}

sdf_na_omit <- function(object, columns = NULL) {
  na <- invoke(object, "na")
  dropped <- if (is.null(columns))
    invoke(na, "drop")
  else
    invoke(na, "drop", as.list(columns))
  dropped
}
