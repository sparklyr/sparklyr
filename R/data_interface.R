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
#'   Supports the \samp{"hdfs://"}, \samp{"s3a://"} and \samp{"file://"} protocols.
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
#' @param ... Optional arguments; currently unused.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}),
#'   as well as the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure to set the following in your spark-defaults.conf
#' \code{spark.hadoop.fs.s3a.access.key}, \code{spark.hadoop.fs.s3a.secret.key} or any of the methods outlined in the aws-sdk
#' documentation \href{http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
#' In order to work with the newer \code{s3a://} protocol also set the values for \code{spark.hadoop.fs.s3a.impl} and \code{spark.hadoop.fs.s3a.endpoint }.
#' In addition, to support v4 of the S3 api be sure to pass the \code{-Dcom.amazonaws.services.s3.enableV4} driver options
#' for the config key \code{spark.driver.extraJavaOptions }
#' For instructions on how to configure \code{s3n://} check the hadoop documentation:
#' \href{https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties}{s3n authentication properties}
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
                           overwrite = TRUE,
                           ...) {
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
#' @param escape The character used to escape other characters, defaults to \code{\\}.
#' @param charset The character set, defaults to \code{"UTF-8"}.
#' @param null_value The character to use for default values, defaults to \code{NULL}.
#' @param options A list of strings with additional options.
#' @param mode A \code{character} element. Specifies the behavior when data or
#'   table already exists. Supported values include: 'error', 'append', 'overwrite' and
#'   ignore. Notice that 'overwrite' will also change the column structure.
#'
#'   For more details see also \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes}
#'   for your version of Spark.
#' @param partition_by A \code{character} vector. Partitions the output by the given columns on the file system.
#' @param ... Optional arguments; currently unused.
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
                            mode = NULL,
                            partition_by = NULL,
                            ...) {
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
                                      mode = NULL,
                                      partition_by = NULL,
                                      ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  options <- spark_csv_options(header, TRUE, delimiter, quote, escape, charset, null_value, options)

  spark_csv_write(sqlResult, spark_normalize_path(path), options, mode, partition_by)
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
                                       mode = NULL,
                                       partition_by = NULL,
                                       ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  options <- spark_csv_options(header, TRUE, delimiter, quote, escape, charset, null_value, options)

  spark_csv_write(x, spark_normalize_path(path), options, mode, partition_by)
}

#' Read a Parquet file into a Spark DataFrame
#'
#' Read a \href{https://parquet.apache.org/}{Parquet} file into a Spark
#' DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param schema A (java) read schema. Useful for optimizing read operation on nested data.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure to set the following in your spark-defaults.conf
#' \code{spark.hadoop.fs.s3a.access.key}, \code{spark.hadoop.fs.s3a.secret.key} or any of the methods outlined in the aws-sdk
#' documentation \href{http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
#' In order to work with the newer \code{s3a://} protocol also set the values for \code{spark.hadoop.fs.s3a.impl} and \code{spark.hadoop.fs.s3a.endpoint }.
#' In addition, to support v4 of the S3 api be sure to pass the \code{-Dcom.amazonaws.services.s3.enableV4} driver options
#' for the config key \code{spark.driver.extraJavaOptions }
#' For instructions on how to configure \code{s3n://} check the hadoop documentation:
#' \href{https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties}{s3n authentication properties}
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
                               overwrite = TRUE,
                               columns = NULL,
                               schema = NULL,
                               ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, list(spark_normalize_path(path)), "parquet", options, columns, schema)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a Parquet file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://parquet.apache.org/}{Parquet} format.
#'
#' @inheritParams spark_write_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_parquet <- function(x,
                                path,
                                mode = NULL,
                                options = list(),
                                partition_by = NULL,
                                ...) {
  UseMethod("spark_write_parquet")
}

#' @export
spark_write_parquet.tbl_spark <- function(x,
                                          path,
                                          mode = NULL,
                                          options = list(),
                                          partition_by = NULL,
                                          ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "parquet", mode, options, partition_by)
}

#' @export
spark_write_parquet.spark_jobj <- function(x,
                                           path,
                                           mode = NULL,
                                           options = list(),
                                           partition_by = NULL,
                                           ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, normalizePath(path), "parquet", mode, options, partition_by)
}

#' Read a JSON file into a Spark DataFrame
#'
#' Read a table serialized in the \href{http://www.json.org/}{JavaScript
#' Object Notation} format into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure to set the following in your spark-defaults.conf
#' \code{spark.hadoop.fs.s3a.access.key}, \code{spark.hadoop.fs.s3a.secret.key} or any of the methods outlined in the aws-sdk
#' documentation \href{http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
#' In order to work with the newer \code{s3a://} protocol also set the values for \code{spark.hadoop.fs.s3a.impl} and \code{spark.hadoop.fs.s3a.endpoint }.
#' In addition, to support v4 of the S3 api be sure to pass the \code{-Dcom.amazonaws.services.s3.enableV4} driver options
#' for the config key \code{spark.driver.extraJavaOptions }
#' For instructions on how to configure \code{s3n://} check the hadoop documentation:
#' \href{https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties}{s3n authentication properties}
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
                            overwrite = TRUE,
                            columns = NULL,
                            ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, spark_normalize_path(path), "json", options, columns)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a JSON file
#'
#' Serialize a Spark DataFrame to the \href{http://www.json.org/}{JavaScript
#' Object Notation} format.
#'
#' @inheritParams spark_write_csv
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_json <- function(x,
                             path,
                             mode = NULL,
                             options = list(),
                             partition_by = NULL,
                             ...) {
  UseMethod("spark_write_json")
}

#' @export
spark_write_json.tbl_spark <- function(x,
                                       path,
                                       mode = NULL,
                                       options = list(),
                                       partition_by = NULL,
                                       ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "json", mode, options, partition_by)
}

#' @export
spark_write_json.spark_jobj <- function(x,
                                        path,
                                        mode = NULL,
                                        options = list(),
                                        partition_by = NULL,
                                        ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, spark_normalize_path(path), "json", mode, options, partition_by)
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

spark_data_read_generic <- function(sc, source, fileMethod, readOptions = list(), columns = NULL, schema = NULL) {
  columnsHaveTypes <- length(names(columns)) > 0
  readSchemaProvided <- !is.null(schema)

  options <- invoke(hive_context(sc), "read")

  lapply(names(readOptions), function(optionName) {
    options <<- invoke(options, "option", optionName, readOptions[[optionName]])
  })

  if (readSchemaProvided) {
    columnDefs <- schema
  } else if (columnsHaveTypes) {
    columnDefs <- spark_data_build_types(sc, columns)
  }
  if (readSchemaProvided || columnsHaveTypes) {
    options <- invoke(options, "schema", columnDefs)
  }

  df <- invoke(options, fileMethod, source)

  if (!columnsHaveTypes && !identical(columns, NULL)) {
    df <- invoke(df, "toDF", as.list(columns))
  }

  df
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

spark_data_write_generic <- function(df,
                                     path,
                                     fileMethod,
                                     mode = NULL,
                                     writeOptions = list(),
                                     partition_by = NULL,
                                     is_jdbc = FALSE) {
  options <- invoke(df, "write")

  options <- spark_data_apply_mode(options, mode)

  lapply(names(writeOptions), function(writeOptionName) {
    options <<- invoke(options, "option", writeOptionName, writeOptions[[writeOptionName]])
  })

  if (!is.null(partition_by))
    options <- invoke(options, "partitionBy", as.list(partition_by))

  if (is_jdbc) {
    sc <- spark_connection(df)

    url <- writeOptions[["url"]]
    writeOptions[["url"]] <- NULL
    if (is.null(url)) stop("Option 'url' is expected while using jdbc")

    properties <- invoke_new(sc, "java.util.Properties")
    lapply(names(writeOptions), function(optionName) {
      invoke(properties, "setProperty", optionName, as.character(writeOptions[[optionName]]))
    })

    invoke(options, fileMethod, url, path, properties)
  }
  else {
    options <- invoke(options, fileMethod, path)
    # Need to call save explicitly in case of generic 'format'
    if(fileMethod == "format") invoke(options, "save")
  }

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
                             overwrite = TRUE,
                             columns = NULL,
                             ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, name, "table", options, columns)
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
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_table <- function(x,
                              name,
                              mode = NULL,
                              options = list(),
                              partition_by = NULL,
                              ...) {
  UseMethod("spark_write_table")
}

#' Saves a Spark DataFrame as a Spark table
#'
#' Saves a Spark DataFrame and as a Spark table.
#'
#' @inheritParams spark_write_csv
#'
#' @family Spark serialization routines
#'
#' @export
spark_save_table <- function(x, path, mode = NULL, options = list()) {
  .Deprecated("spark_write_table")
  spark_write_table(x, path, mode, options)
}

#' @export
spark_write_table.tbl_spark <- function(x,
                                        name,
                                        mode = NULL,
                                        options = list(),
                                        partition_by = NULL,
                                        ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  sc <- spark_connection(x)

  if (spark_version(sc) < "2.0.0" && spark_master_is_local(sc$master)) {
    stop(
      "spark_write_table is not supported in local clusters for Spark ",
      spark_version(sc), ". ",
      "Upgrade to Spark 2.X or use this function in a non-local Spark cluster.")
  }

  fileMethod <- ifelse(identical(mode, "append"), "insertInto", "saveAsTable")

  spark_data_write_generic(sqlResult, name, fileMethod, mode, options, partition_by)
}

#' @export
spark_write_table.spark_jobj <- function(x,
                                         name,
                                         mode = NULL,
                                         options = list(),
                                         partition_by = NULL,
                                         ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  sc <- spark_connection(x)

  spark_data_write_generic(x, name, "saveAsTable", mode, options, partition_by)
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
                             overwrite = TRUE,
                             columns = NULL,
                             ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, "jdbc", "format", options, columns) %>% invoke("load")
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Read libsvm file into a Spark DataFrame.
#'
#' Read libsvm file into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_libsvm <- function(sc,
                            name,
                            path,
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE,
                            ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, "libsvm", "format") %>%
    invoke("load", spark_normalize_path(path))
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Read from a generic source into a Spark DataFrame.
#'
#' Read from a generic source into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param source A data source capable of reading data.
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_source <- function(sc,
                            name,
                            source,
                            options = list(),
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE,
                            columns = NULL,
                            ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, source, "format", options, columns) %>% invoke("load")
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Writes a Spark DataFrame into a JDBC table
#'
#' Writes a Spark DataFrame into a JDBC table.
#'
#' @inheritParams spark_write_csv
#' @param name The name to assign to the newly generated table.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_jdbc <- function(x,
                             name,
                             mode = NULL,
                             options = list(),
                             partition_by = NULL,
                             ...) {
  UseMethod("spark_write_jdbc")
}

#' @export
spark_write_jdbc.tbl_spark <- function(x,
                                       name,
                                       mode = NULL,
                                       options = list(),
                                       partition_by = NULL,
                                       ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  sc <- spark_connection(x)

  spark_data_write_generic(sqlResult, name, "jdbc", mode, options, partition_by, is_jdbc = TRUE)
}

#' @export
spark_write_jdbc.spark_jobj <- function(x,
                                        name,
                                        mode = NULL,
                                        options = list(),
                                        partition_by = NULL,
                                        ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  sc <- spark_connection(x)

  spark_data_write_generic(x, name, "jdbc", mode, options, partition_by, is_jdbc = TRUE)
}

#' Writes a Spark DataFrame into a generic source
#'
#' Writes a Spark DataFrame into a generic source.
#'
#' @inheritParams spark_write_csv
#' @param source A data source capable of reading data.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_source <- function(x,
                               source,
                               mode = NULL,
                               options = list(),
                               partition_by = NULL,
                               ...) {
  UseMethod("spark_write_source")
}

#' @export
spark_write_source.tbl_spark <- function(x,
                                         source,
                                         mode = NULL,
                                         options = list(),
                                         partition_by = NULL,
                                         ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, source, "format", mode, options, partition_by)
}

#' @export
spark_write_source.spark_jobj <- function(x,
                                          source,
                                          mode = NULL,
                                          options = list(),
                                          partition_by = NULL,
                                          ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, source, "format", mode, options, partition_by)
}

#' Read a Text file into a Spark DataFrame
#'
#' Read a text file into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure to set the following in your spark-defaults.conf
#' \code{spark.hadoop.fs.s3a.access.key}, \code{spark.hadoop.fs.s3a.secret.key} or any of the methods outlined in the aws-sdk
#' documentation \href{http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
#' In order to work with the newer \code{s3a://} protocol also set the values for \code{spark.hadoop.fs.s3a.impl} and \code{spark.hadoop.fs.s3a.endpoint }.
#' In addition, to support v4 of the S3 api be sure to pass the \code{-Dcom.amazonaws.services.s3.enableV4} driver options
#' for the config key \code{spark.driver.extraJavaOptions }
#' For instructions on how to configure \code{s3n://} check the hadoop documentation:
#' \href{https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties}{s3n authentication properties}
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_text <- function(sc,
                            name,
                            path,
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE,
                            ...) {

  if (overwrite) spark_remove_table_if_exists(sc, name)

  columns = list(line = "character")

  df <- spark_data_read_generic(sc, list(spark_normalize_path(path)), "text", options, columns)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a Text file
#'
#' Serialize a Spark DataFrame to the plain text format.
#'
#' @inheritParams spark_write_csv
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_text <- function(x,
                             path,
                             mode = NULL,
                             options = list(),
                             partition_by = NULL,
                             ...) {
  UseMethod("spark_write_text")
}

#' @export
spark_write_text.tbl_spark <- function(x,
                                       path,
                                       mode = NULL,
                                       options = list(),
                                       partition_by = NULL,
                                       ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "text", mode, options, partition_by)
}

#' @export
spark_write_text.spark_jobj <- function(x,
                                        path,
                                        mode = NULL,
                                        options = list(),
                                        partition_by = NULL,
                                        ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, spark_normalize_path(path), "text", mode, options, partition_by)
}
