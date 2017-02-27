# Helper functions to support dplyr sql operations

# dplyr_s3 @export
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

# dplyr_s3 @export
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
