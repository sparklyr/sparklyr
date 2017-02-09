

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

  # deserialize columns as needed (string columns will enter as
  # a single newline-delimited string)
  transformed <- lapply(collected, sdf_deserialize_column)

  # fix an issue where sometimes columns in a Spark DataFrame are empty
  # in such a case, we fill those with NAs of the same type (#477)
  n <- vapply(transformed, length, numeric(1))
  rows <- max(n)
  fixed <- lapply(transformed, function(column) {
    if (length(column) == 0) {
      converter <- switch(
        typeof(column),
        character = as.character,
        logical   = as.logical,
        integer   = as.integer,
        double    = as.double,
        identity
      )
      return(converter(rep(NA, rows)))
    }
    column
  })

  # set column names and return dataframe
  colNames <- invoke(sdf, "columns")
  names(fixed) <- as.character(colNames)
  dplyr::as_data_frame(fixed, stringsAsFactors = FALSE, optional = TRUE)
}

# Split a Spark DataFrame
sdf_split <- function(object,
                      weights = c(0.5, 0.5),
                      seed = sample(.Machine$integer.max, 1))
{
  jobj <- spark_dataframe(object)
  invoke(jobj, "randomSplit", as.list(weights), as.integer(seed))
}

#' Pivot a Spark DataFrame
#'
#' Construct a pivot table over a Spark Dataframe, using a syntax similar to
#' that from \code{reshape2::dcast}.
#'
#' @template roxlate-ml-x
#' @param formula A two-sided \R formula of the form \code{x_1 + x_2 + ... ~ y_1}.
#'   The left-hand side of the formula indicates which variables are used for grouping,
#'   and the right-hand side indicates which variable is used for pivoting. Currently,
#'   only a single pivot column is supported.
#' @param fun.aggregate How should the grouped dataset be aggregated? Can be
#'   a length-one character vector, giving the name of a Spark aggregation function
#'   to be called; a named \R list mapping column names to an aggregation method,
#'   or an \R function that is invoked on the grouped dataset.
#' @export
sdf_pivot <- function(x, formula, fun.aggregate = "count") {
  sdf <- spark_dataframe(x)

  # parse formulas of form "abc + def ~ ghi + jkl"
  deparsed <- paste(deparse(formula), collapse = " ")
  splat <- strsplit(deparsed, "~", fixed = TRUE)[[1]]
  if (length(splat) != 2)
    stop("expected a two-sided formula; got '", deparsed, "'")

  grouped_cols <- trim_whitespace(strsplit(splat[[1]], "[+*]")[[1]])
  pivot_cols <- trim_whitespace(strsplit(splat[[2]], "[+*]", fixed = TRUE)[[1]])

  # ensure no duplication of variables on each side
  intersection <- intersect(grouped_cols, pivot_cols)
  if (length(intersection))
    stop("variables on both sides of forumla: ", paste(deparse(intersection), collapse = " "))

  # ensure variables exist in dataset
  nm <- as.character(invoke(sdf, "columns"))
  all_cols <- c(grouped_cols, pivot_cols)
  missing_cols <- setdiff(all_cols, nm)
  if (length(missing_cols))
    stop("missing variables in dataset: ", paste(deparse(missing_cols), collapse = " "))

  # ensure pivot is length one (for now)
  if (length(pivot_cols) != 1)
    stop("pivot column is not length one")

  # generate pivoted dataset
  grouped <- sdf %>%
    invoke("groupBy", grouped_cols[[1]], as.list(grouped_cols[-1])) %>%
    invoke("pivot", pivot_cols[[1]])

  # perform aggregation
  fun.aggregate <- fun.aggregate %||% "count"
  result <- if (is.function(fun.aggregate)) {
    fun.aggregate(grouped)
  } else if (is.character(fun.aggregate)) {
    if (length(fun.aggregate) == 1)
      invoke(grouped, fun.aggregate[[1]])
    else
      invoke(grouped, fun.aggregate[[1]], as.list(fun.aggregate[-1]))
  } else if (is.list(fun.aggregate)) {
    invoke(grouped, "agg", list2env(fun.aggregate))
  } else {
    stop("unsupported 'fun.aggregate' type '", class(fun.aggregate)[[1]], "'")
  }

  sdf_register(result)
}
