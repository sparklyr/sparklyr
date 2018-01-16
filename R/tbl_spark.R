#' @export
dim.tbl_spark <- function(x) {
  c(NA_real_, sdf_ncol(x))
}

#' @export
type_sum.spark_jobj <- function(x) {
  paste0(jobj_info(x)$repr)
}
