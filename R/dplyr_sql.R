# Helper functions to support dplyr sql operations

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_n <- function(op, con, ...) {
  select_query(
    from = sql(paste(
      sql_build(op$x, con = con),
      " TABLESAMPLE (",
      as.integer(op$args$size),
      " rows)", sep = "")),
    select = build_sql("*")
  )
}

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_frac <- function(op, con, ...) {
  select_query(
    from = sql(paste(
      sql_build(op$x, con = con),
      " TABLESAMPLE (",
      op$args$size * 100,
      " PERCENT)", sep = "")),
    select = build_sql("*")
  )
}
