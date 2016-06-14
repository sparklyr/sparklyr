# Utilities for interacting with Spark DataFrames (sql.Dataset)s.

#' Convert a tbl to a Spark Dataset.
#'
#' @param x A \code{spark_tbl}.
#' @param ... Additional parameters.
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

spark_dataframe_schema <- function(object) {
  jobj <- as_spark_dataframe(object)
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

spark_dataframe_read_column <- function(object, colName) {
  jobj <- as_spark_dataframe(object)
  schema <- spark_dataframe_schema(jobj)
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

  scon <- spark_scon(jobj)
  column <- spark_invoke_static(scon, "utils", method, jobj, colName)

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

spark_dataframe_cast_column <- function(df, input_col, output_col, output_type)
{
  spark_invoke_static(

    "utils",
    "castColumn",

    as_spark_dataframe(df),
    input_col,
    output_col,
    output_type
  )
}

#' Read a Spark Dataset into R.
#' @param object An (object convertable to a) Spark DataFrame.
#' @export
spark_dataframe_collect <- function(object) {
  jobj <- as_spark_dataframe(object)
  schema <- spark_dataframe_schema(jobj)
  colNames <- as.character(spark_invoke(jobj, "columns"))
  colValues <- lapply(schema, function(colInfo) {
    spark_dataframe_read_column(jobj, colInfo$name)
  })

  df <- lapply(colValues, unlist, recursive = FALSE)
  names(df) <- colNames
  dplyr::as_data_frame(df, stringsAsFactors = FALSE, optional = TRUE)
}

#' Split a Spark DataFrame
#'
#' @param object An (object convertable to a) Spark DataFrame.
#' @param weights A numeric vector of weights.
#' @param seed A numeric seed.
#'
#' @export
spark_dataframe_split <- function(object,
                                  weights = c(0.5, 0.5),
                                  seed = sample(.Machine$integer.max, 1))
{
  jobj <- as_spark_dataframe(object)
  spark_invoke(jobj, "randomSplit", as.list(weights), as.integer(seed))
}

