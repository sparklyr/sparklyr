#' @include avro_utils.R
#' @include sdf_interface.R
#' @include spark_apply.R

gen_sdf_name <- function(path, config) {
  path %>%
    basename() %>%
    tools::file_path_sans_ext() %>%
    spark_sanitize_names(config) %>%
    random_string()
}

# This function handles backward compatibility to support
# unnamed datasets while not breaking sparklyr 0.9 param
# signature. Returns a c(name, path) tuple.
spark_read_compat_param <- function(sc, name, path) {
  if (is.null(name) && is.null(path)) {
    stop("The 'path' parameter must be specified.")
  } else if (identical(name, path)) {
    # This is an invalid use case, for 'spark_read_*(sc, "hello")';
    # however, for convenience and backwards compatibility we allow
    # to use the second parameter as the path.
    c(gen_sdf_name(name, sc$config), name)
  } else if (identical(name, NULL)) {
    c(gen_sdf_name(path, sc$config), path)
  } else {
    c(name, path)
  }
}

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
      header = ifelse(identical(header, TRUE), "true", "false"),
      inferSchema = ifelse(identical(inferSchema, TRUE), "true", "false"),
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
#'   If specified, the elements can be \code{"binary"} for \code{BinaryType},
#'   \code{"boolean"} for \code{BooleanType}, \code{"byte"} for \code{ByteType},
#'   \code{"integer"} for \code{IntegerType}, \code{"integer64"} for \code{LongType},
#'   \code{"double"} for \code{DoubleType}, \code{"character"} for \code{StringType},
#'   \code{"timestamp"} for \code{TimestampType} and \code{"date"} for \code{DateType}.
#' @param infer_schema Boolean; should column types be automatically inferred?
#'   Requires one extra pass over the data. Defaults to \code{is.null(columns)}.
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
#' documentation \href{https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
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
                           name = NULL,
                           path = name,
                           header = TRUE,
                           columns = NULL,
                           infer_schema = is.null(columns),
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
  c(name, path) %<-% spark_read_compat_param(sc, name, path)

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
    columns
  )

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
#' @param quote The character used as a quote. Defaults to \samp{'"'}.
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
#' documentation \href{https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
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
                               name = NULL,
                               path = name,
                               options = list(),
                               repartition = 0,
                               memory = TRUE,
                               overwrite = TRUE,
                               columns = NULL,
                               schema = NULL,
                               ...) {
  params <- spark_read_compat_param(sc, name, path)
  name <- params[1L]
  path <- params[-1L]
  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, as.list(spark_normalize_path(path)), "parquet", options, columns, schema)
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
  spark_data_write_generic(x, spark_normalize_path(path), "parquet", mode, options, partition_by)
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
#' documentation \href{https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
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
                            name = NULL,
                            path = name,
                            options = list(),
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE,
                            columns = NULL,
                            ...) {
  params <- spark_read_compat_param(sc, name, path)
  name <- params[1L]
  path <- params[-1L]
  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, as.list(spark_normalize_path(path)), "json", options, columns)
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
  className <- invoke(jobj, "%>%", list("getClass"), list("getName"))
  if (!identical(className, expectedClassName)) {
    stop(
      "This operation is only supported on ",
      expectedClassName, " jobjs but found ",
      className, " instead."
    )
  }
}

spark_data_read_generic <- function(sc, source, fileMethod, readOptions = list(), columns = NULL, schema = NULL) {
  columnsHaveTypes <- length(names(columns)) > 0
  readSchemaProvided <- !is.null(schema)

  options <- hive_context(sc) %>|%
    append(
      list(list("read")),
      lapply(
        names(readOptions),
        function(optionName) list("option", optionName, readOptions[[optionName]])
      )
    )

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
    options <- (
      if (is.list(mode)) {
        options %>|% lapply(mode, function(m) list("mode", m))
      } else if (is.character(mode)) {
        invoke(options, "mode", mode)
      } else {
        stop("Unsupported type ", typeof(mode), " for mode parameter.")
      }
    )
  }

  options
}

spark_data_write_generic <- function(df,
                                     path,
                                     fileMethod,
                                     mode = NULL,
                                     writeOptions = list(),
                                     partition_by = NULL,
                                     is_jdbc = FALSE,
                                     save_args = list()) {
  options <- invoke(df, "write") %>%
    spark_data_apply_mode(mode) %>|%
    lapply(names(writeOptions), function(writeOptionName) {
      list("option", writeOptionName, writeOptions[[writeOptionName]])
    })

  if (!is.null(partition_by)) {
    options <- invoke(options, "partitionBy", as.list(partition_by))
  }

  if (is_jdbc) {
    sc <- spark_connection(df)

    url <- writeOptions[["url"]]
    writeOptions[["url"]] <- NULL
    if (is.null(url)) stop("Option 'url' is expected while using jdbc")

    properties <- invoke_new(sc, "java.util.Properties") %>|%
      lapply(names(writeOptions), function(optionName) {
        list("setProperty", optionName, as.character(writeOptions[[optionName]]))
      })

    invoke(options, fileMethod, url, path, properties)
  }
  else {
    options <- invoke(options, fileMethod, path)
    # Need to call save explicitly in case of generic 'format'
    if (fileMethod == "format") do.call(invoke, c(options, "save", save_args))
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
                             columns = NULL,
                             ...) {
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
      "Upgrade to Spark 2.X or use this function in a non-local Spark cluster."
    )
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
                              name = NULL,
                              path = name,
                              repartition = 0,
                              memory = TRUE,
                              overwrite = TRUE,
                              ...) {
  c(name, path) %<-% spark_read_compat_param(sc, name, path)
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
                              name = NULL,
                              path = name,
                              source,
                              options = list(),
                              repartition = 0,
                              memory = TRUE,
                              overwrite = TRUE,
                              columns = NULL,
                              ...) {
  c(name, path) %<-% spark_read_compat_param(sc, name, path)
  if (overwrite) spark_remove_table_if_exists(sc, name)

  df_reader <- spark_data_read_generic(sc, source, "format", options, columns)
  df <- if (is.null(path)) invoke(df_reader, "load") else invoke(df_reader, "load", spark_normalize_path(path))
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
#' @examples
#' \dontrun{
#' sc <- spark_connect(
#'   master = "local",
#'   config = list(sparklyr.shell.packages = "mysql:mysql-connector-java:5.1.44")
#' )
#' spark_write_jdbc(
#'   sdf_len(sc, 10),
#'   name = "my_sql_table",
#'   options = list(
#'     url = "jdbc:mysql://localhost:3306/my_sql_schema",
#'     user = "me",
#'     password = "******",
#'     dbtable = "my_sql_table"
#'   )
#' )
#' }
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
  spark_data_write_generic(sqlResult, source, "format", mode, options, partition_by, ...)
}

#' @export
spark_write_source.spark_jobj <- function(x,
                                          source,
                                          mode = NULL,
                                          options = list(),
                                          partition_by = NULL,
                                          ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, source, "format", mode, options, partition_by, ...)
}

#' Read a Text file into a Spark DataFrame
#'
#' Read a text file into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param whole Read the entire text file as a single entry? Defaults to \code{FALSE}.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' If you are reading from a secure S3 bucket be sure to set the following in your spark-defaults.conf
#' \code{spark.hadoop.fs.s3a.access.key}, \code{spark.hadoop.fs.s3a.secret.key} or any of the methods outlined in the aws-sdk
#' documentation \href{https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html}{Working with AWS credentials}
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
                            name = NULL,
                            path = name,
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE,
                            options = list(),
                            whole = FALSE,
                            ...) {
  params <- spark_read_compat_param(sc, name, path)
  name <- params[1L]
  path <- params[-1L]
  if (overwrite) spark_remove_table_if_exists(sc, name)

  columns <- list(line = "character")

  if (identical(whole, TRUE)) {
    if (length(path) != 1L) stop("spark_read_text is only suppored with path of length 1 if whole=TRUE.")
    path_field <- invoke_static(sc, "sparklyr.SQLUtils", "createStructField", "path", "character", TRUE)
    contents_field <- invoke_static(sc, "sparklyr.SQLUtils", "createStructField", "contents", "character", TRUE)
    schema <- invoke_static(sc, "sparklyr.SQLUtils", "createStructType", list(path_field, contents_field))

    rdd <- invoke_static(sc, "sparklyr.Utils", "readWholeFiles", spark_context(sc), path)
    df <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  }
  else {
    df <- spark_data_read_generic(sc, as.list(spark_normalize_path(path)), "text", options, columns)
  }

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

#' Read a ORC file into a Spark DataFrame
#'
#' Read a \href{https://orc.apache.org/}{ORC} file into a Spark
#' DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param schema A (java) read schema. Useful for optimizing read operation on nested data.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_orc <- function(sc,
                           name = NULL,
                           path = name,
                           options = list(),
                           repartition = 0,
                           memory = TRUE,
                           overwrite = TRUE,
                           columns = NULL,
                           schema = NULL,
                           ...) {
  params <- spark_read_compat_param(sc, name, path)
  name <- params[1L]
  path <- params[-1L]

  if (length(path) != 1L && (spark_version(sc) < "2.0.0")) {
    stop("spark_read_orc is only suppored with path of length 1 for spark versions < 2.0.0")
  }

  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, as.list(spark_normalize_path(path)), "orc", options, columns, schema)
  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to a ORC file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://orc.apache.org/}{ORC} format.
#'
#' @inheritParams spark_write_csv
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_orc <- function(x,
                            path,
                            mode = NULL,
                            options = list(),
                            partition_by = NULL,
                            ...) {
  UseMethod("spark_write_orc")
}

#' @export
spark_write_orc.tbl_spark <- function(x,
                                      path,
                                      mode = NULL,
                                      options = list(),
                                      partition_by = NULL,
                                      ...) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_generic(sqlResult, spark_normalize_path(path), "orc", mode, options, partition_by)
}

#' @export
spark_write_orc.spark_jobj <- function(x,
                                       path,
                                       mode = NULL,
                                       options = list(),
                                       partition_by = NULL,
                                       ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_generic(x, spark_normalize_path(path), "orc", mode, options, partition_by)
}

#' Writes a Spark DataFrame into Delta Lake
#'
#' Writes a Spark DataFrame into Delta Lake.
#'
#' @inheritParams spark_write_csv
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_delta <- function(x,
                              path,
                              mode = NULL,
                              options = list(),
                              partition_by = NULL,
                              ...) {
  options$path <- path
  spark_write_source(x, "delta", mode = mode, options = options, partition_by = partition_by)
}

#' Read from Delta Lake into a Spark DataFrame.
#'
#' Read from Delta Lake into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param version The version of the delta table to read.
#' @param timestamp The timestamp of the delta table to read. For example,
#'   \code{"2019-01-01"} or \code{"2019-01-01'T'00:00:00.000Z"}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_delta <- function(sc,
                             path,
                             name = NULL,
                             version = NULL,
                             timestamp = NULL,
                             options = list(),
                             repartition = 0,
                             memory = TRUE,
                             overwrite = TRUE,
                             ...) {
  if (!is.null(version)) options$versionAsOf <- version
  if (!is.null(timestamp)) options$timestampAsOf <- timestamp

  spark_read_source(sc,
    name = name,
    path = path,
    source = "delta",
    options = options,
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
  )
}

#' Read Apache Avro data into a Spark DataFrame.
#'
#' Read Apache Avro data into a Spark DataFrame.
#' Notice this functionality requires the Spark connection \code{sc} to be instantiated with either
#' an explicitly specified Spark version (i.e.,
#' \code{spark_connect(..., version = <version>, packages = c("avro", <other package(s)>), ...)})
#' or a specific version of Spark avro package to use (e.g.,
#' \code{spark_connect(..., packages = c("org.apache.spark:spark-avro_2.12:3.0.0", <other package(s)>), ...)}).
#'
#' @inheritParams spark_read_csv
#' @param avro_schema Optional Avro schema in JSON format
#' @param ignore_extension If enabled, all files with and without .avro extension
#'   are loaded (default: \code{TRUE})
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_avro <- function(sc,
                            name = NULL,
                            path = name,
                            avro_schema = NULL,
                            ignore_extension = TRUE,
                            repartition = 0,
                            memory = TRUE,
                            overwrite = TRUE) {
  validate_spark_avro_pkg_version(sc)

  options <- list()
  if (!is.null(avro_schema)) {
    if (!is.character(avro_schema)) {
      stop("Expect Avro schema to be a JSON string")
    }

    options$avroSchema <- avro_schema
  }
  options$ignoreExtension <- ignore_extension

  spark_read_source(sc,
    name = name,
    path = path,
    source = "avro",
    options = options,
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
  )
}

#' Serialize a Spark DataFrame into Apache Avro format
#'
#' Serialize a Spark DataFrame into Apache Avro format.
#' Notice this functionality requires the Spark connection \code{sc} to be instantiated with either
#' an explicitly specified Spark version (i.e.,
#' \code{spark_connect(..., version = <version>, packages = c("avro", <other package(s)>), ...)})
#' or a specific version of Spark avro package to use (e.g.,
#' \code{spark_connect(..., packages = c("org.apache.spark:spark-avro_2.12:3.0.0", <other package(s)>), ...)}).
#'
#' @inheritParams spark_write_csv
#' @param avro_schema Optional Avro schema in JSON format
#' @param record_name Optional top level record name in write result (default: "topLevelRecord")
#' @param record_namespace Record namespace in write result (default: "")
#' @param compression Compression codec to use (default: "snappy")
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_avro <- function(x,
                             path,
                             avro_schema = NULL,
                             record_name = "topLevelRecord",
                             record_namespace = "",
                             compression = "snappy",
                             partition_by = NULL) {
  validate_spark_avro_pkg_version(spark_connection(x))

  options <- list()
  if (!is.null(avro_schema)) {
    if (!is.character(avro_schema)) {
      stop("Expect Avro schema to be a JSON string")
    }

    options$avroSchema <- avro_schema
  }
  options$recordName <- record_name
  options$recordNamespace <- record_namespace
  options$compression <- compression

  spark_write_source(
    x,
    "avro",
    options = options,
    partition_by = partition_by,
    save_args = list(path)
  )
}

#' Read file(s) into a Spark DataFrame using a custom reader
#'
#' Run a custom R function on Spark workers to ingest data from one or more files
#' into a Spark DataFrame, assuming all files follow the same schema.
#'
#' @param sc A \code{spark_connection}.
#' @param paths A character vector of one or more file URIs (e.g.,
#'   c("hdfs://localhost:9000/file.txt", "hdfs://localhost:9000/file2.txt"))
#' @param reader A self-contained R function that takes a single file URI as
#'   argument and returns the data read from that file as a data frame.
#' @param columns a named list of column names and column types of the resulting
#'   data frame (e.g., list(column_1 = "integer", column_2 = "character")), or a
#'   list of column names only if column types should be inferred from the data
#'   (e.g., list("column_1", "column_2"), or NULL if column types should be
#'   inferred and resulting data frame can have arbitrary column names
#' @param packages A list of R packages to distribute to Spark workers
#' @param ... Optional arguments; currently unused.
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(
#'   master = "yarn",
#'   spark_home = "~/spark/spark-2.4.5-bin-hadoop2.7"
#' )
#'
#' # This is a contrived example to show reader tasks will be distributed across
#' # all Spark worker nodes
#' spark_read(
#'   sc,
#'   rep("/dev/null", 10),
#'   reader = function(path) system("hostname", intern = TRUE),
#'   columns = c(hostname = "string")
#' ) %>% sdf_collect()
#' }
#'
#' @family Spark serialization routines
#'
#' @export
spark_read <- function(sc,
                       paths,
                       reader,
                       columns,
                       packages = TRUE,
                       ...) {
  assert_that(is.function(reader) || is.language(reader))

  args <- list(...)
  paths <- lapply(paths, as.list)
  if (!identical(names(columns), NULL)) {
    # If columns is of the form c("col_name1", "col_name2", ...)
    # then leave it as-is
    # Otherwise if it is of the form c(col_name1 = "col_type1", ...)
    # or list(col_name1 = "col_type1", ...), etc, then make sure it gets coerced
    columns <- as.list(columns)
  }

  rdd_base <- invoke_static(
    sc,
    "sparklyr.Utils",
    "createDataFrame",
    spark_context(sc),
    paths,
    as.integer(length(paths))
  )

  if (is.language(reader)) f <- rlang::as_closure(reader)
  reader <- serialize(reader, NULL)
  worker_impl <- function(df, rdr) {
    rdr <- unserialize(rdr)
    do.call(rbind, lapply(df$path, function(path) rdr(path)))
  }
  worker_impl <- serialize(worker_impl, NULL)

  worker_port <- as.integer(
    spark_config_value(sc$config, "sparklyr.gateway.port", "8880")
  )

  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) packages <- FALSE

  bundle_path <- get_spark_apply_bundle_path(sc, packages)

  serialized_worker_context <- serialize(
    list(column_types = list("character"), user_context = reader), NULL
  )

  rdd <- invoke_static(
    sc,
    "sparklyr.WorkerHelper",
    "computeRdd",
    rdd_base,
    worker_impl,
    spark_apply_worker_config(
      sc,
      args$debug,
      args$profile
    ),
    as.integer(worker_port),
    list("path"),
    list(),
    raw(),
    bundle_path,
    new.env(),
    as.integer(60),
    serialized_worker_context,
    new.env()
  )
  rdd <- invoke(rdd, "cache")
  schema <- spark_schema_from_rdd(sc, rdd, columns)
  sdf <- invoke(hive_context(sc), "createDataFrame", rdd, schema) %>%
    sdf_register()

  sdf
}

#' Write Spark DataFrame to RDS files
#'
#' Write Spark dataframe to RDS files. Each partition of the dataframe will be
#' exported to a separate RDS file so that all partitions can be processed in
#' parallel.
#'
#' @param x A Spark DataFrame to be exported
#' @param dest_uri  Can be a URI template containing "{partitionId}" (e.g.,
#'   "hdfs://my_data_part_{partitionId}.rds") where "{partitionId}" will be
#'   substituted with ID of each partition using `glue`, or a list of URIs
#'   to be assigned to RDS output from all partitions (e.g.,
#'   "hdfs://my_data_part_0.rds", "hdfs://my_data_part_1.rds", and so on)
#'   If working with a Spark instance running locally, then all URIs should be
#'   in "file://<local file path>" form. Otherwise the scheme of the URI should
#'   reflect the underlying file system the Spark instance is working with
#'   (e.g., "hdfs://"). If the resulting list of URI(s) does not contain unique
#'   values, then it will be post-processed with `make.unique()` to ensure
#'   uniqueness.
#'
#' @return A tibble containing partition ID and RDS file location for each
#'   partition of the input Spark dataframe.
#'
#' @export
spark_write_rds <- function(x, dest_uri) {
  sc <- spark_connection(x)
  if (spark_version(sc) < "2.0.0") {
    stop("`spark_write_rds()` is only supported in Spark 2.0 or above")
  }

  num_partitions <- sdf_num_partitions(x)
  if (length(dest_uri) == 1) {
    dest_uri <- lapply(
      seq(num_partitions) - 1,
      function(part_id) {
        glue::glue(dest_uri, partitionId = part_id)
      }
    ) %>%
      unlist()
  }
  dest_uri <- dest_uri %>% make.unique()
  if (num_partitions != length(dest_uri)) {
    stop(
      "Number of destination URI(s) does not match the number of partitions ",
      "in input Spark dataframe (",
      length(dest_uri),
      " vs. ",
      num_partitions
    )
  }

  invoke_static(
    spark_connection(x),
    "sparklyr.RDSCollector",
    "collect",
    spark_dataframe(x),
    as.list(dest_uri)
  )

  tibble::tibble(
    partition_id = seq(num_partitions) - 1,
    uri = dest_uri
  )
}

#' Write Spark DataFrame to file using a custom writer
#'
#' Run a custom R function on Spark worker to write a Spark DataFrame
#' into file(s). If Spark's speculative execution feature is enabled (i.e.,
#' `spark.speculation` is true), then each write task may be executed more than
#' once and the user-defined writer function will need to ensure no concurrent
#' writes happen to the same file path (e.g., by appending UUID to each file name).
#'
#' @param x A Spark Dataframe to be saved into file(s)
#' @param writer A writer function with the signature function(partition, path)
#'   where \code{partition} is a R dataframe containing all rows from one partition
#'   of the original Spark Dataframe \code{x} and path is a string specifying the
#'   file to write \code{partition} to
#' @param paths A single destination path or a list of destination paths, each one
#'   specifying a location for a partition from \code{x} to be written to. If
#'   number of partition(s) in \code{x} is not equal to \code{length(paths)} then
#'   \code{x} will be re-partitioned to contain \code{length(paths)} partition(s)
#' @param packages Boolean to distribute \code{.libPaths()} packages to each node,
#'   a list of packages to distribute, or a package bundle created with
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local[3]")
#'
#' # copy some test data into a Spark Dataframe
#' sdf <- sdf_copy_to(sc, iris, overwrite = TRUE)
#'
#' # create a writer function
#' writer <- function(df, path) {
#'   write.csv(df, path)
#' }
#'
#' spark_write(
#'   sdf,
#'   writer,
#'   # re-partition sdf into 3 partitions and write them to 3 separate files
#'   paths = list("file:///tmp/file1", "file:///tmp/file2", "file:///tmp/file3"),
#' )
#'
#' spark_write(
#'   sdf,
#'   writer,
#'   # save all rows into a single file
#'   paths = list("file:///tmp/all_rows")
#' )
#' }
#'
#' @export
spark_write <- function(x,
                        writer,
                        paths,
                        packages = NULL) {
  UseMethod("spark_write")
}

#' @export
spark_write.tbl_spark <- function(x,
                                  writer,
                                  paths,
                                  packages = NULL) {
  if (length(paths) == 0) {
    stop("'paths' must contain at least 1 path")
  }

  assert_that(is.function(writer) || is.language(writer))

  paths <- spark_normalize_path(paths)
  dst_num_partitions <- length(paths)
  src_num_partitions <- sdf_num_partitions(x)

  if (!identical(src_num_partitions, dst_num_partitions)) {
    x <- sdf_repartition(x, dst_num_partitions)
  }

  spark_apply(
    x,
    function(df, ctx, partition_index) {
      # Spark partition index is 0-based
      ctx$writer(df, ctx$paths[[partition_index + 1]])
    },
    context = list(writer = writer, paths = as.list(paths)),
    packages = packages,
    fetch_result_as_sdf = FALSE,
    partition_index_param = "partition_index"
  )
}

#' @export
spark_write.spark_jobj <- function(x,
                                   writer,
                                   paths,
                                   packages = NULL) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  x %>%
    sdf_register() %>%
    spark_write(writer, paths, packages)
}
