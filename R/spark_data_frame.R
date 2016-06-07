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

spark_dataframe_read_column <- function(jobj, colName, colType) {

  method <- if (colType == "DoubleType")
    "readColumnDouble"
  else if (colType == "IntegerType")
    "readColumnInt"
  else if (colType == "BooleanType")
    "readColumnBoolean"
  else if (colType == "StringType")
    "readColumnString"
  else
    "readColumnDefault"

  column <- spark_invoke_static(jobj$scon, "utils", method, jobj, colName)
  if (colType == "StringType")
    column <- strsplit(column, ",", fixed = TRUE)[[1]]
  column
}

#' Read a Spark Dataset into R.
#' @param jobj The \code{jobj} underlying a Spark Dataset.
#' @export
spark_collect <- function(jobj) {
  schema <- spark_dataframe_schema(jobj)
  colNames <- as.character(spark_invoke(jobj, "columns"))
  colValues <- lapply(schema, function(colInfo) {
    spark_dataframe_read_column(jobj, colInfo$name, colInfo$type)
  })

  df <- lapply(colValues, unlist, recursive = FALSE)
  names(df) <- colNames
  dplyr::as_data_frame(df, stringsAsFactors = FALSE, optional = TRUE)
}
