#' Invoke distinct on a Spark DataFrame
#'
#' @template roxlate-sdf
#'
#' @param x A Spark DataFrame.
#' @param ... Optional variables to use when determining uniqueness.
#'   If there are multiple rows for a given combination of inputs,
#'   only the first row will be preserved. If omitted, will use all
#'   variables.
#' @param name A name to assign this table. Passed to [sdf_register()].
#' @family Spark data frames
#' @export
sdf_distinct <- function(x, ..., name) {
  UseMethod("sdf_distinct")
}

#' @export
sdf_distinct.tbl_spark <- function(x, ..., name = NULL) {
  if (rlang::dots_n(...) > 0L) {
    x <- dplyr::select(x, ...)
  }
  x %>%
    spark_dataframe() %>%
    invoke("distinct") %>%
    sdf_register(name = name)
}

#' @export
sdf_distinct.spark_jobj <- function(x, ..., name = NULL) {
  invoke(x, "distinct")
}
