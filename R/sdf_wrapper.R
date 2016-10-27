

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

#' Read the Schema of a Spark DataFrame
#'
#' Read the schema of a Spark DataFrame.
#'
#' The \code{type} column returned gives the string representation of the
#' underlying Spark  type for that column; for example, a vector of numeric
#' values would be returned with the type \code{"DoubleType"}. Please see the
#' \href{http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.package}{Spark Scala API Documentation}
#' for information on what types are available and exposed by Spark.
#'
#' @return An \R \code{list}, with each \code{list} element describing the
#'   \code{name} and \code{type} of a column.
#'
#' @template roxlate-ml-x
#'
#' @export
sdf_schema <- function(x) {
  jobj <- spark_dataframe(x)
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

#' Read a Column from a Spark DataFrame
#'
#' Read a single column from a Spark DataFrame, and return
#' the contents of that column back to \R.
#'
#' @template roxlate-ml-x
#' @param column The name of a column within \code{x}.
#' @export
sdf_read_column <- function(x, column) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  schema <- sdf_schema(sdf)
  colType <- schema[[column]]$type

  column <- sc %>%
    invoke_static("sparklyr.Utils", "collectColumn", sdf, column, colType) %>%
    sdf_deserialize_column()

  column
}

# Read a Spark Dataset into R.
sdf_collect <- function(object) {
  sc <- spark_connection(object)
  sdf <- spark_dataframe(object)

  # for some reason, we appear to receive invalid results when
  # collecting Spark DataFrames with many columns. empirically,
  # having more than 50 columns seems to trigger the buggy behavior
  # collect the data set in chunks, and then join those chunks.
  # note that this issue should be resolved with Spark >2.0.0
  collected <- if (spark_version(sc) > "2.0.0") {
    invoke_static(sc, "sparklyr.Utils", "collect", sdf)
  } else {
    columns <- invoke(sdf, "columns") %>% as.character()
    chunk_size <- getOption("sparklyr.collect.chunk.size", default = 50L)
    chunks <- split_chunks(columns, as.integer(chunk_size))
    pieces <- lapply(chunks, function(chunk) {
      subset <- sdf %>% invoke("selectExpr", as.list(chunk))
      invoke_static(sc, "sparklyr.Utils", "collect", subset)
    })
    do.call(c, pieces)
  }

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

