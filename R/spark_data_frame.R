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
  list <- lapply(fields, function(field) {
    type <- spark_invoke(spark_invoke(field, "dataType"), "toString")
    name <- spark_invoke(field, "name")
    list(name = name, type = type)
  })
  names(list) <- unlist(lapply(list, `[[`, "name"))
  list
}

spark_dataframe_read_column <- function(dataFrame, colName) {
  dataFrame <- as_spark_dataframe(dataFrame)
  schema <- spark_dataframe_schema(dataFrame)
  colType <- schema[[colName]]$type

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

  scon <- spark_scon(dataFrame)
  column <- spark_invoke_static(scon, "utils", method, dataFrame, colName)

  if (colType == "StringType") {

    df <- readr::read_csv(
      column,
      col_names = FALSE,
      col_types = "c",
      na = character(),
      trim_ws = FALSE,
      progress = FALSE
    )

    column <- df[[1]]
    Encoding(column) <- "UTF-8"
  }

  column
}

#' Read a Spark Dataset into R.
#' @param jobj The \code{jobj} underlying a Spark Dataset.
#' @export
spark_collect <- function(jobj) {
  schema <- spark_dataframe_schema(jobj)
  colNames <- as.character(spark_invoke(jobj, "columns"))
  colValues <- lapply(schema, function(colInfo) {
    spark_dataframe_read_column(jobj, colInfo$name)
  })

  df <- lapply(colValues, unlist, recursive = FALSE)
  names(df) <- colNames
  dplyr::as_data_frame(df, stringsAsFactors = FALSE, optional = TRUE)
}
