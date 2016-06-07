# Utilities for interacting with Spark DataFrames (sql.Dataset)s.

#' Convert a tbl to a Spark Dataset.
#'
#' @param x A \code{spark_tbl}.
#' @export
as_spark_dataframe <- function(x, ...) {
  UseMethod("as_spark_dataframe")
}

#' @export
as_spark_dataframe.jobj <- function(x, ...) {
  x
}

#' @export
as_spark_dataframe.tbl_spark <- function(x, ...) {
  db <- x$src
  con <- db$con

  sql <- as.character(sql_render(sql_build(x, con = con), con = con))
  api <- spark_sql_or_hive(spark_api(x$src))
  spark_invoke(api, "sql", sql)
}

spark_dataframe_schema <- function(jobj) {
  schema <- spark_invoke(jobj, "schema")
  fields <- spark_invoke(schema, "fields")
  lapply(fields, function(field) {
    type <- spark_invoke(spark_invoke(field, "dataType"), "toString")
    name <- spark_invoke(field, "name")
    list(name = name, type = type)
  })
}

#' Read a Spark Dataset into R.
#' @param jobj The \code{jobj} underlying a Spark Dataset.
#' @export
spark_collect <- function(jobj) {
  scon <- jobj$scon

  colNames <- as.character(spark_invoke(jobj, "columns"))
  colValues <- spark_invoke_static(
    scon,
    "org.apache.spark.sql.api.r.SQLUtils",
    "dfToCols",
    jobj
  )

  df <- lapply(colValues, unlist, recursive = FALSE)
  names(df) <- colNames
  dplyr::as_data_frame(df, stringsAsFactors = FALSE, optional = TRUE)
}
