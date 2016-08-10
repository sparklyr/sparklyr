

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

sdf_deserialize_column <- function(column) {
  if (is.character(column)) {
    splat <- strsplit(column, "\n", fixed = TRUE)[[1]]
    splat[splat == "<NA>"] <- NA
    Encoding(splat) <- "UTF-8"
    return(splat)
  }

  column
}

sdf_read_column <- function(object, colName) {
  sdf <- spark_dataframe(object)
  schema <- sdf_schema(sdf)
  colType <- schema[[colName]]$type

  column <- sc %>%
    invoke_static("sparklyr.Utils", "collectColumn", sdf, colName, colType) %>%
    sdf_deserialize_column()

  column
}

# Read a Spark Dataset into R.
sdf_collect <- function(object) {
  sdf <- spark_dataframe(object)
  collected <- invoke_static(sc, "sparklyr.Utils", "collect", sdf)
  transformed <- lapply(collected, sdf_deserialize_column)
  colNames <- invoke(sdf, "columns")
  names(transformed) <- as.character(colNames)
  dplyr::as_data_frame(transformed, stringsAsFactors = FALSE, optional = TRUE)
}

# Split a Spark DataFrame
sdf_split <- function(object,
                      weights = c(0.5, 0.5),
                      seed = sample(.Machine$integer.max, 1))
{
  jobj <- spark_dataframe(object)
  invoke(jobj, "randomSplit", as.list(weights), as.integer(seed))
}

