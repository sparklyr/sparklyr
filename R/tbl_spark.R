#' @export
dim.tbl_spark <- function(x) {
  sdf <- spark_dataframe(x)
  rows <- invoke(sdf, "count")
  columns <- invoke(sdf, "columns")
  c(rows, length(columns))
}

#' @export
type_sum.spark_jobj <- function(x) {
  paste0(jobj_info(x)$repr)
}
