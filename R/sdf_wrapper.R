

#' @export
spark_dataframe.tbl_spark <- function(x, ...) {
  sc <- spark_connection(x)

  sql <- as.character(sql_render(sql_build(x, con = sc), con = sc))
  hive <- hive_context(sc)
  invoke(hive, "sql", sql)
}

#' @export
spark_dataframe.spark_connection <- function(x, sql = NULL, ...) {
  invoke(hive_context(x), "sql", as.character(sql))
}

sdf_schema <- function(object) {
  jobj <- spark_dataframe(object)
  schema <- invoke(jobj, "schema")
  fields <- invoke(schema, "fields")
  list <- lapply(fields, function(field) {
    type <- invoke(invoke(field, "dataType"), "toString")
    name <- invoke(field, "name")
    list(name = name, type = type)
  })
  names(list) <- unlist(lapply(list, `[[`, "name"))
  list
}

sdf_read_column <- function(object, colName) {
  jobj <- spark_dataframe(object)
  schema <- sdf_schema(jobj)
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

  sc <- spark_connection(jobj)
  rdd <- jobj %>%
    invoke("select", colName, list()) %>%
    invoke("rdd")

  column <- invoke_static(sc, "sparklyr.Utils", method, rdd)

  if (colType == "StringType") {
    column <- strsplit(column, "\n", fixed = TRUE)[[1]]
    column[column == "<NA>"] <- NA
    Encoding(column) <- "UTF-8"
  }

  column
}

# Read a Spark Dataset into R.
sdf_collect <- function(object) {
  jobj <- spark_dataframe(object)
  schema <- sdf_schema(jobj)
  colNames <- as.character(invoke(jobj, "columns"))
  colValues <- lapply(schema, function(colInfo) {
    sdf_read_column(jobj, colInfo$name)
  })

  df <- lapply(colValues, unlist, recursive = FALSE)
  names(df) <- colNames
  dplyr::as_data_frame(df, stringsAsFactors = FALSE, optional = TRUE)
}

# Split a Spark DataFrame
sdf_split <- function(object,
                      weights = c(0.5, 0.5),
                      seed = sample(.Machine$integer.max, 1))
{
  jobj <- spark_dataframe(object)
  invoke(jobj, "randomSplit", as.list(weights), as.integer(seed))
}

