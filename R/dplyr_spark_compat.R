#' @rname dplyr-spark-compat
#' Compatibility functions to support current and future dplyr versions

if (!exists("sql_build")) {
  sql_build <- build_sql
}
