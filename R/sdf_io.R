#' Save / Load a Spark DataFrame
#'
#' Routines for saving and loading Spark DataFrames.
#'
#' @param sc A \code{spark_connection} object.
#' @template roxlate-ml-x
#' @param path The path where the Spark DataFrame should be saved.
#' @param name The table name to assign to the saved Spark DataFrame.
#' @param overwrite Boolean; overwrite a pre-existing table of the same name?
#' @param append Boolean; append to a pre-existing table of the same name?
#'
#' @rdname sdf-saveload
#' @name sdf-saveload
NULL

#' @rdname sdf-saveload
#' @export
sdf_save_table <- function(x, name, overwrite = FALSE, append = FALSE) {
  .Deprecated("spark_write_table")

  sdf <- spark_dataframe(x)
  name <- cast_string(name)

  writer <- invoke(sdf, "write")
  if (overwrite) {
    writer <- invoke(writer, "mode", "overwrite")
  }
  if (append) {
    writer <- invoke(writer, "mode", "append")
  }

  # Spark < 2.0.0 doesn't respect the metastore directory when
  # using the 'saveAsTable' API, so we directly call 'save'.
  sc <- spark_connection(sdf)
  if (spark_version(sc) < "2.0.0") {
    hc <- hive_context(sc)
    metastore <- invoke(hc, "getConf", "hive.metastore.warehouse.dir")
    path <- path.expand(file.path(metastore, name))
    invoke(writer, "save", path)
  } else {
    invoke(writer, "saveAsTable", name)
  }
}

#' @rdname sdf-saveload
#' @export
sdf_load_table <- function(sc, name) {
  .Deprecated("spark_read_table")

  session <- spark_session(sc)
  name <- cast_string(name)

  # NOTE: need to explicitly provide path to metastore for
  # Spark < 2.0.0
  reader <- invoke(session, "read")
  sdf <- if (spark_version(sc) < "2.0.0") {
    hc <- hive_context(sc)
    metastore <- invoke(hc, "getConf", "hive.metastore.warehouse.dir")
    path <- file.path(metastore, name)
    invoke(reader, "load", path)
  } else {
    invoke(reader, "table", name)
  }

  sdf_register(sdf)
}

#' @rdname sdf-saveload
#' @export
sdf_save_parquet <- function(x, path, overwrite = FALSE, append = FALSE) {
  .Deprecated("spark_write_parquet")

  sdf <- spark_dataframe(x)
  path <- cast_string(path)

  write <- invoke(sdf, "write")
  if (overwrite) {
    write <- invoke(write, "mode", "overwrite")
  }
  if (append) {
    write <- invoke(write, "mode", "append")
  }

  invoke(write, "parquet", path)
}

#' @rdname sdf-saveload
#' @export
sdf_load_parquet <- function(sc, path) {
  .Deprecated("spark_read_parquet")

  session <- spark_session(sc)
  path <- as.character(path)

  sdf <- session %>%
    invoke("read") %>%
    invoke("parquet", as.list(path))

  sdf_register(sdf)
}

df_from_sql <- function(sc, sql) {
  sdf <- invoke(hive_context(sc), "sql", as.character(sql))
  df_from_sdf(sc, sdf)
}

df_from_sdf <- function(sc, sdf, take = -1) {
  collect(sdf)
}

#' Spark DataFrame from SQL
#'
#' Defines a Spark DataFrame from a SQL query, useful to create Spark DataFrames
#' without collecting the results immediately.
#'
#' @param sc A \code{spark_connection}.
#' @param sql a 'SQL' query used to generate a Spark DataFrame.
#'
#' @export
sdf_sql <- function(sc, sql) {
  hive_context(sc) %>%
    invoke("sql", as.character(sql)) %>%
    sdf_register()
}

#' Create DataFrame for Length
#'
#' Creates a DataFrame for the given length.
#'
#' @param sc The associated Spark connection.
#' @param length The desired length of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_len <- function(
  sc,
  length,
  repartition = NULL,
  type = c("integer", "integer64")
) {
  sdf_seq(sc, 1, length, repartition = repartition, type = type)
}

#' Create DataFrame for Range
#'
#' Creates a DataFrame for the given range
#'
#' @param sc The associated Spark connection.
#' @param from,to The start and end to use as a range
#' @param by The increment of the sequence.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster. Defaults to the minimum number of partitions.
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_seq <- function(
  sc,
  from = 1L,
  to = 1L,
  by = 1L,
  repartition = NULL,
  type = c("integer", "integer64")
) {
  from <- cast_scalar_integer(from)
  to <- cast_scalar_integer(to + 1)
  by <- cast_scalar_integer(by)

  # validate bits parameter
  type <- match.arg(type)

  type_map <- list(
    "integer" = "Integer",
    "integer64" = "Long"
  )
  type_name <- type_map[[type]]

  if (is.null(repartition)) {
    repartition <- invoke(spark_context(sc), "defaultMinPartitions")
  }
  repartition <- cast_scalar_integer(repartition)

  rdd <- invoke(spark_context(sc), "range", from, to, by, repartition)
  rdd <- invoke_static(
    sc,
    "sparklyr.Utils",
    paste0("mapRdd", type_name, "ToRddRow"),
    rdd
  )

  schema <- invoke_static(
    sc,
    "sparklyr.Utils",
    paste0("buildStructTypeFor", type_name, "Field")
  )
  sdf <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(sdf)
}

#' Create DataFrame for along Object
#'
#' Creates a DataFrame along the given object.
#'
#' @param sc The associated Spark connection.
#' @param along Takes the length from the length of this argument.
#' @param repartition The number of partitions to use when distributing the
#'   data across the Spark cluster.
#' @param type The data type to use for the index, either \code{"integer"} or \code{"integer64"}.
#'
#' @export
sdf_along <- function(
  sc,
  along,
  repartition = NULL,
  type = c("integer", "integer64")
) {
  sdf_len(sc, length(along), repartition = repartition, type = type)
}

#' Support for Dimension Operations
#'
#' \code{sdf_dim()},  \code{sdf_nrow()} and \code{sdf_ncol()} provide similar
#' functionality to \code{dim()}, \code{nrow()} and \code{ncol()}.
#'
#' @param x An object (usually a \code{spark_tbl}).
#' @name sdf_dim
NULL

#' @export
#' @rdname sdf_dim
sdf_dim <- function(x) {
  sdf <- spark_dataframe(x)
  rows <- invoke(sdf, "count")
  columns <- invoke(sdf, "columns")
  c(rows, length(columns))
}

#' @export
#' @rdname sdf_dim
sdf_nrow <- function(x) invoke(spark_dataframe(x), "count")

#' @export
#' @rdname sdf_dim
sdf_ncol <- function(x) length(invoke(spark_dataframe(x), "columns"))

#' Invoke distinct on a Spark DataFrame
#'
#'
#' @param x A Spark DataFrame.
#' @param ... Optional variables to use when determining uniqueness.
#'   If there are multiple rows for a given combination of inputs,
#'   only the first row will be preserved. If omitted, will use all
#'   variables.
#' @param name A name to assign this table. Passed to [sdf_register()].
#' @family Spark data frames
#' @export
sdf_distinct <- function(x, ..., name) {
  UseMethod("sdf_distinct")
}

#' @export
sdf_distinct.tbl_spark <- function(x, ..., name = NULL) {
  if (rlang::dots_n(...) > 0L) {
    x <- dplyr::select(x, ...)
  }
  x %>%
    spark_dataframe() %>%
    invoke("distinct") %>%
    sdf_register(name = name)
}

#' @export
sdf_distinct.spark_jobj <- function(x, ..., name = NULL) {
  invoke(x, "distinct")
}
