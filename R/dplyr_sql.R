# Helper functions to support dplyr sql operations

spark_sql_count_rows <- function(op, con) {
  countQuery <- select_spark_query(
    from = sql_build(op$x, con = con),
    select = sql("count(*) as rowcount")
  )

  countSql <- sql_render(countQuery, con = con)
  countResult <- dbGetQuery(con, countSql)

  countResult[[1]][[1]]
}

#' @export
sql_build.op_sample_n <- function(op, con, ...) {
  select_spark_query(
    from = sql(paste(
      sql_build(op$x, con = con),
      " TABLESAMPLE (",
      as.integer(op$args$size),
      " rows)", sep = "")),
    select = build_sql("*")
  )
}

#' @export
sql_build.op_sample_frac <- function(op, con, ...) {
  select_spark_query(
    from = sql(paste(
      sql_build(op$x, con = con),
      " TABLESAMPLE (",
      op$args$size,
      " PERCENT)", sep = "")),
    select = build_sql("*")
  )
}
