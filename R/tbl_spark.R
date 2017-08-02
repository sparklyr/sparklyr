#' @export
dim.tbl_spark <- function(x) {
  NA
}

#' @export
type_sum.spark_jobj <- function(x) {
  paste0(jobj_info(x)$repr)
}
