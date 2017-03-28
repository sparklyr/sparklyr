spark_csv_options <- function(header,
                              inferSchema,
                              delimiter,
                              quote,
                              escape,
                              charset,
                              nullValue,
                              options) {
  c(
    options,
    list(
      header = ifelse(header, "true", "false"),
      inferSchema = ifelse(inferSchema, "true", "false"),
      delimiter = toString(delimiter),
      quote = toString(quote),
      escape = toString(escape),
      charset = toString(charset),
      nullValue = toString(nullValue)
    )
  )
}

#' Read a CSV file into a Spark DataFrame
#'
#' Read a tabular data file into a Spark DataFrame.
#'
#' @param sc A \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3n://"} and \samp{"file://"} protocols.
#' @param memory Boolean; should the data be loaded eagerly into memory? (That
#'   is, should the table be cached?)
#' @param header Boolean; should the first row of data be used as a header?
#'   Defaults to \code{TRUE}.
#' @param columns A vector of column names or a named vector of column types.
#' @param infer_schema Boolean; should column types be automatically inferred?
#'   Requires one extra pass over the data. Defaults to \code{TRUE}.
#' @param delimiter The character used to delimit each column. Defaults to \samp{','}.
#' @param quote The character used as a quote. Defaults to \samp{'"'}.
#' @param escape The character used to escape other characters. Defaults to \samp{'\'}.
#' @param charset The character set. Defaults to \samp{"UTF-8"}.
#' @param null_value The character to use for null, or missing, values. Defaults to \code{NULL}.
#' @param options A list of strings with additional options.
#' @param repartition The number of partitions used to distribute the
#'   generated table. Use 0 (the default) to avoid partitioning.
#' @param overwrite Boolean; overwrite the table with the given name if it
#'   already exists?
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3n://}),
#'   as well as the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure that the
#' \code{AWS_ACCESS_KEY_ID} and \code{AWS_SECRET_ACCESS_KEY} environment
#' variables are both defined.
#'
#' When \code{header} is \code{FALSE}, the column names are generated with a
#' \code{V} prefix; e.g. \code{V1, V2, ...}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_csv <- function(sc,
                           name,
                           path,
                           header = TRUE,
                           columns = NULL,
                           infer_schema = TRUE,
                           delimiter = ",",
                           quote = "\"",
                           escape = "\\",
                           charset = "UTF-8",
                           null_value = NULL,
                           options = list(),
                           repartition = 0,
                           memory = TRUE,
                           overwrite = TRUE) {
  columnsHaveTypes <- length(names(columns)) > 0
  if (!identical(columns, NULL) & isTRUE(infer_schema) & columnsHaveTypes) {
    stop("'infer_schema' must be set to FALSE when 'columns' specifies column types")
  }

  if (overwrite) spark_remove_table_if_exists(sc, name)

  options <- spark_csv_options(header, infer_schema, delimiter, quote, escape, charset, null_value, options)
  df <- spark_csv_read(
    sc,
    spark_normalize_path(path),
    options,
    if (columnsHaveTypes) columns else NULL)

  if ((identical(columns, NULL) & identical(header, FALSE)) |
      (!identical(columns, NULL) & !columnsHaveTypes)) {
    newNames <- if (!identical(columns, NULL)) {
      columns
    }
    else {
      # create normalized column names when header = FALSE and a columns specification is not supplied
      columns <- invoke(df, "columns")
      n <- length(columns)
      sprintf("V%s", seq_len(n))
    }
    df <- invoke(df, "toDF", as.list(newNames))
  } else {
    # sanitize column names
    colNames <- as.character(invoke(df, "columns"))
    sanitized <- spark_sanitize_names(colNames)
    if (!identical(colNames, sanitized))
      df <- invoke(df, "toDF", as.list(sanitized))
  }

  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a CSV
#'
#' Write a Spark DataFrame to a tabular (typically, comma-separated) file.
#'
#' @inheritParams spark_read_csv
#' @param x A Spark DataFrame or dplyr operation
#' @param header Should the first row of data be used as a header? Defaults to \code{TRUE}.
#' @param delimiter The character used to delimit each column, defaults to \code{,}.
#' @param quote The character used as a quote, defaults to \code{"hdfs://"}.
#' @param escape The chatacter used to escape other characters, defaults to \code{\\}.
#' @param charset The character set, defaults to \code{"UTF-8"}.
#' @param null_value The character to use for default values, defaults to \code{NULL}.
#' @param options A list of strings with additional options.
#' @param mode Specifies the behavior when data or table already exists.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_csv <- function(x, path,
                            header = TRUE,
                            delimiter = ",",
                            quote = "\"",
                            escape = "\\",
                            charset = "UTF-8",
                            null_value = NULL,
                            options = list(),
                            mode = NULL) {
  UseMethod("spark_write_csv")
}

#' @export
spark_write_csv.tbl_spark <- function(x,
                                      path,
                                      header = TRUE,
                                      delimiter = ",",
                                      quote = "\"",
                                      escape = "\\",
                                      charset = "UTF-8",
                                      null_value = NULL,
                                      options = list(),
                                      mode = NULL) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  options <- spark_csv_options(header, TRUE, delimiter, quote, escape, charset, null_value, options)

  spark_csv_write(sqlResult, spark_normalize_path(path), options, mode)
}

#' @export
spark_write_csv.spark_jobj <- function(x,
                                       path,
                                       header = TRUE,
                                       delimiter = ",",
                                       quote = "\"",
                                       escape = "\\",
                                       charset = "UTF-8",
                                       null_value = NULL,
                                       options = list(),
                                       mode = NULL) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  options <- spark_csv_options(header, TRUE, delimiter, quote, escape, charset, null_value, options)

  spark_csv_write(x, spark_normalize_path(path), options, mode)
}

#' Read a Parquet file into a Spark DataFrame
#'
#' Read a \href{https://parquet.apache.org/}{Parquet} file into a Spark
#' DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3n://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure that the \code{AWS_ACCESS_KEY_ID} and
#'   \code{AWS_SECRET_ACCESS_KEY} environment variables are both defined.
#'
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_parquet <- function(sc,
                               name,
                               path,
                               options = list(),
                               repartition = 0,
                               memory = TRUE,
                               overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, list(spark_normalize_path(path)), "parquet", options)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a Parquet file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://parquet.apache.org/}{Parquet} format.
#'
#' @inheritParams spark_write_csv
#' @param mode Specifies the behavior when data or table already exists.
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_parquet <- function(x, path, mode = NULL, options = list()) {
  UseMethod("spark_write_parquet")
}

#' @export
spark_write_parquet.tbl_spark <- function(x, path, mode = NULL, options = list()) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "parquet", mode, options)
}

#' @export
spark_write_parquet.spark_jobj <- function(x, path, mode = NULL, options = list()) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, normalizePath(path), "parquet", mode, options)
}

#' Read a JSON file into a Spark DataFrame
#'
#' Read a table serialized in the \href{http://www.json.org/}{JavaScript
#' Object Notation} format into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3n://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure that the \code{AWS_ACCESS_KEY_ID} and
#'   \code{AWS_SECRET_ACCESS_KEY} environment variables are both defined.
#'
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_json <- function(sc,
                            name,
                            path,
                            options = list(),
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, spark_normalize_path(path), "json", options)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a JSON file
#'
#' Serialize a Spark DataFrame to the \href{http://www.json.org/}{JavaScript
#' Object Notation} format.
#'
#' @inheritParams spark_write_csv
#' @param mode Specifies the behavior when data or table already exists.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_json <- function(x, path, mode = NULL, options = list()) {
  UseMethod("spark_write_json")
}

#' @export
spark_write_json.tbl_spark <- function(x, path, mode = NULL, options = list()) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "json", mode, options)
}

#' @export
spark_write_json.spark_jobj <- function(x, path, mode = NULL, options = list()) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, spark_normalize_path(path), "json", mode, options)
}

spark_expect_jobj_class <- function(jobj, expectedClassName) {
  class <- invoke(jobj, "getClass")
  className <- invoke(class, "getName")
  if (!identical(className, expectedClassName)) {
    stop(paste(
      "This operation is only supported on",
      expectedClassName,
      "jobjs but found",
      className,
      "instead.")
    )
  }
}

spark_data_read_generic <- function(sc, path, fileMethod, readOptions = list()) {
  options <- invoke(hive_context(sc), "read")

  lapply(names(readOptions), function(optionName) {
    options <<- invoke(options, "option", optionName, readOptions[[optionName]])
  })

  invoke(options, fileMethod, path)
}

spark_data_apply_mode <- function(options, mode) {
  if (!is.null(mode)) {
    if (is.list(mode)) {
      lapply(mode, function(m) {
        options <<- invoke(options, "mode", m)
      })
    }
    else if (is.character(mode)) {
      options <- invoke(options, "mode", mode)
    }
    else {
      stop("Unsupported type ", typeof(mode), " for mode parameter.")
    }
  }

  options
}

spark_data_write_generic <- function(df, path, fileMethod, mode = NULL, csvOptions = list()) {
  options <- invoke(df, "write")

  options <- spark_data_apply_mode(options, mode)

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  invoke(options, fileMethod, path)
  invisible(TRUE)
}

#' Reads from a Spark Table into a Spark DataFrame.
#'
#' Reads from a Spark Table into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_table <- function(sc,
                             name,
                             options = list(),
                             repartition = 0,
                             memory = TRUE,
                             overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, name, "table", options)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Reads from a Spark Table into a Spark DataFrame.
#'
#' Reads from a Spark Table into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_load_table <- function(sc,
                             name,
                             path,
                             options = list(),
                             repartition = 0,
                             memory = TRUE,
                             overwrite = TRUE) {
  .Deprecated("spark_read_table")
  spark_read_table(
    sc,
    name,
    options,
    repartition,
    memory,
    overwrite
  )
}

#' Writes a Spark DataFrame into a Spark table
#'
#' Writes a Spark DataFrame into a Spark table.
#'
#' @inheritParams spark_write_csv
#' @param name The name to assign to the newly generated table.
#' @param mode Specifies the behavior when data or table already exists.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_table <- function(x, name, mode = NULL, options = list()) {
  UseMethod("spark_write_table")
}

#' Saves a Spark DataFrame as a Spark table
#'
#' Saves a Spark DataFrame and as a Spark table.
#'
#' @inheritParams spark_write_csv
#' @param mode Specifies the behavior when data or table already exists.
#'
#' @family Spark serialization routines
#'
#' @export
spark_save_table <- function(x, path, mode = NULL, options = list()) {
  .Deprecated("spark_write_table")
  spark_write_table(x, path, mode, options)
}

#' @export
spark_write_table.tbl_spark <- function(x, name, mode = NULL, options = list()) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  sc <- spark_connection(x)

  if (spark_version(sc) < "2.0.0" && spark_master_is_local(sc$master)) {
    stop(
      "spark_write_table is not supported in local clusters for Spark ",
      spark_version(sc), ". ",
      "Upgrade to Spark 2.X or use this function in a non-local Spark cluster.")
  }

  spark_data_write_generic(sqlResult, name, "saveAsTable", mode, options)
}

#' @export
spark_write_table.spark_jobj <- function(x, name, mode = NULL, options = list()) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  sc <- spark_connection(x)

  spark_data_write_generic(x, name, "saveAsTable", mode, options)
}

#' Read from JDBC connection into a Spark DataFrame.
#'
#' Read from JDBC connection into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_jdbc <- function(sc,
                             name,
                             options = list(),
                             repartition = 0,
                             memory = TRUE,
                             overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, "jdbc", "format", options) %>% invoke("load")
  spark_partition_register_df(sc, df, name, repartition, memory)
}
