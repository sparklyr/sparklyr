#' @include arrow.R
#' @include sdf_interface.R
#' @include spark_apply.R
#' @include spark_data.R

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


spark_read_prepare_target <- function(sc, name, path, overwrite) {
  name_provided <- !is.null(name)
  params <- spark_read_compat_param(sc, name, path)
  name <- params[1L]
  path <- params[-1L]
  if (overwrite && name_provided) {
    spark_remove_table_if_exists(sc, name)
  }
  list(name = name, path = path)
}


spark_csv_options <- function(
  header,
  inferSchema,
  delimiter,
  quote,
  escape,
  charset,
  nullValue,
  options
) {
  c(
    options,
    list(
      header = if (identical(header, TRUE)) "true" else "false",
      inferSchema = if (identical(inferSchema, TRUE)) "true" else "false",
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
#' When \code{header} is \code{FALSE}, the column names are generated with a
#' \code{V} prefix; e.g. \code{V1, V2, ...}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_csv <- function(
  sc,
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
  ...
) {
  UseMethod("spark_read_csv")
}


#' @export
spark_read_csv.spark_connection <- function(
  sc,
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
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  columnsHaveTypes <- length(names(columns)) > 0
  if (!identical(columns, NULL) & isTRUE(infer_schema) & columnsHaveTypes) {
    stop(
      "'infer_schema' must be set to FALSE when 'columns' specifies column types"
    )
  }

  options <- spark_csv_options(
    header,
    infer_schema,
    delimiter,
    quote,
    escape,
    charset,
    null_value,
    options
  )
  df <- spark_csv_read(
    sc,
    spark_normalize_path(path),
    options,
    columns
  )

  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read a Parquet file into a Spark DataFrame
#'
#' Read a \href{https://parquet.apache.org/}{Parquet} file into a Spark
#' DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param schema A (java) read schema. Useful for optimizing read operation on nested data.
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_parquet <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  schema = NULL,
  ...
) {
  UseMethod("spark_read_parquet")
}


#' @export
spark_read_parquet.spark_connection <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  schema = NULL,
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  df <- spark_data_read_generic(
    sc,
    as.list(spark_normalize_path(path)),
    "parquet",
    options,
    columns,
    schema
  )
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read a JSON file into a Spark DataFrame
#'
#' Read a table serialized in the \href{https://www.json.org/}{JavaScript
#' Object Notation} format into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#'
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_json <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  ...
) {
  UseMethod("spark_read_json")
}


#' @export
spark_read_json.spark_connection <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  df <- spark_data_read_generic(
    sc,
    as.list(spark_normalize_path(path)),
    "json",
    options,
    columns
  )
  spark_partition_register_df(sc, df, name, repartition, memory)
}


spark_data_read_generic <- function(
  sc,
  source,
  fileMethod,
  readOptions = list(),
  columns = NULL,
  schema = NULL
) {
  columnsHaveTypes <- length(names(columns)) > 0
  readSchemaProvided <- !is.null(schema)

  options <- hive_context(sc) %>|%
    append(
      list(list("read")),
      lapply(
        names(readOptions),
        function(optionName) {
          list("option", optionName, readOptions[[optionName]])
        }
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
    options <- (if (is.list(mode)) {
      options %>|% lapply(mode, function(m) list("mode", m))
    } else if (is.character(mode)) {
      invoke(options, "mode", mode)
    } else {
      stop("Unsupported type ", typeof(mode), " for mode parameter.")
    })
  }

  options
}


spark_data_write_generic <- function(
  df,
  path,
  fileMethod,
  mode = NULL,
  writeOptions = list(),
  partition_by = NULL,
  is_jdbc = FALSE,
  save_args = list()
) {
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
    if (is.null(url)) {
      stop("Option 'url' is expected while using jdbc")
    }

    properties <- invoke_static(
      sc,
      "sparklyr.Utils",
      "setProperties",
      as.list(names(writeOptions)),
      unname(writeOptions)
    )

    invoke(options, fileMethod, url, path, properties)
  } else {
    options <- invoke(options, fileMethod, path)
    # Need to call save explicitly in case of generic 'format'
    if (fileMethod == "format") do.call(invoke, c(options, "save", save_args))
  }

  invisible(TRUE)
}


#' Reads from a Spark Table into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options.
#' See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @family Spark serialization routines
#' @export
spark_read_table <- function(
  sc,
  name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  columns = NULL,
  ...
) {
  df <- spark_data_read_generic(sc, name, "table", options, columns)
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Reads from a Spark Table into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options. See
#' \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @family Spark serialization routines
#' @export
spark_load_table <- function(
  sc,
  name,
  path,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE
) {
  .Deprecated("spark_read_table")
  spark_read_table(
    sc,
    name,
    options = options,
    repartition = repartition,
    memory = memory
  )
}


#' Read from JDBC connection into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options.
#' See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @family Spark serialization routines
#' @examples
#' \dontrun{
#' sc <- spark_connect(
#'   master = "local",
#'   config = list(
#'     `sparklyr.shell.driver-class-path` = "/usr/share/java/mysql-connector-java-8.0.25.jar"
#'   )
#' )
#' spark_read_jdbc(
#'   sc,
#'   name = "my_sql_table",
#'   options = list(
#'     url = "jdbc:mysql://localhost:3306/my_sql_schema",
#'     driver = "com.mysql.jdbc.Driver",
#'     user = "me",
#'     password = "******",
#'     dbtable = "my_sql_table"
#'   )
#' )
#' }
#'
#' @export
spark_read_jdbc <- function(
  sc,
  name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  ...
) {
  if (overwrite) {
    spark_remove_table_if_exists(sc, name)
  }

  df <- spark_data_read_generic(sc, "jdbc", "format", options, columns) %>%
    invoke("load")
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read libsvm file into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @family Spark serialization routines
#' @export
spark_read_libsvm <- function(
  sc,
  name = NULL,
  path = name,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  options = list(),
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  df <- spark_data_read_generic(sc, "libsvm", "format", options) %>%
    invoke("load", spark_normalize_path(path))
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read from a generic source into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param source A data source capable of reading data.
#' @param options A list of strings with additional options.
#' See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @family Spark serialization routines
#' @export
spark_read_source <- function(
  sc,
  name = NULL,
  path = name,
  source,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  df_reader <- spark_data_read_generic(sc, source, "format", options, columns)
  df <- if (is.null(path)) {
    invoke(df_reader, "load")
  } else {
    if (sc$method != "databricks-connect") {
      # `path` refers to some remote file in 'databricks-connect' use case, so,
      # it should not be normalized based on the local OS platform or filesystem
      path <- spark_normalize_path(path)
    }
    invoke(df_reader, "load", path)
  }
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read a Text file into a Spark DataFrame
#' @inheritParams spark_read_csv
#' @param whole Read the entire text file as a single entry? Defaults to \code{FALSE}.
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#' @family Spark serialization routines
#' @export
spark_read_text <- function(
  sc,
  name = NULL,
  path = name,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  options = list(),
  whole = FALSE,
  ...
) {
  UseMethod("spark_read_text")
}


#' @export
spark_read_text.spark_connection <- function(
  sc,
  name = NULL,
  path = name,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  options = list(),
  whole = FALSE,
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  columns <- list(line = "character")

  if (identical(whole, TRUE)) {
    if (length(path) != 1L) {
      stop(
        "spark_read_text is only suppored with path of length 1 if whole=TRUE."
      )
    }
    path_field <- invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructField",
      "path",
      "character",
      TRUE
    )
    contents_field <- invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructField",
      "contents",
      "character",
      TRUE
    )
    schema <- invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructType",
      list(path_field, contents_field)
    )

    rdd <- invoke_static(
      sc,
      "sparklyr.Utils",
      "readWholeFiles",
      spark_context(sc),
      path
    )
    df <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  } else {
    df <- spark_data_read_generic(
      sc,
      as.list(spark_normalize_path(path)),
      "text",
      options,
      columns
    )
  }

  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read a ORC file into a Spark DataFrame
#'
#' Read a \href{https://orc.apache.org/}{ORC} file into a Spark
#' DataFrame.
#' @inheritParams spark_read_csv
#' @param options A list of strings with additional options.
#' See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @param schema A (java) read schema. Useful for optimizing read operation on nested data.
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3a://}), as well as
#'   the local file system (\code{file://}).
#' @family Spark serialization routines
#' @export
spark_read_orc <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  schema = NULL,
  ...
) {
  UseMethod("spark_read_orc")
}


#' @export
spark_read_orc.spark_connection <- function(
  sc,
  name = NULL,
  path = name,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  columns = NULL,
  schema = NULL,
  ...
) {
  target <- spark_read_prepare_target(sc, name, path, overwrite)
  name <- target$name
  path <- target$path

  if (length(path) != 1L && (spark_version(sc) < "2.0.0")) {
    stop(
      "spark_read_orc is only suppored with path of length 1 for spark versions < 2.0.0"
    )
  }

  df <- spark_data_read_generic(
    sc,
    as.list(spark_normalize_path(path)),
    "orc",
    options,
    columns,
    schema
  )
  spark_partition_register_df(sc, df, name, repartition, memory)
}


#' Read from Delta Lake into a Spark DataFrame.
#'
#' Read from Delta Lake into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param version The version of the delta table to read.
#' @param timestamp The timestamp of the delta table to read. For example,
#'   \code{"2019-01-01"} or \code{"2019-01-01'T'00:00:00.000Z"}.
#' @family Spark serialization routines
#' @export
spark_read_delta <- function(
  sc,
  path,
  name = NULL,
  version = NULL,
  timestamp = NULL,
  options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE,
  ...
) {
  if (!is.null(version)) {
    options$versionAsOf <- version
  }
  if (!is.null(timestamp)) {
    options$timestampAsOf <- timestamp
  }

  spark_read_source(
    sc,
    name = name,
    path = path,
    source = "delta",
    options = options,
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
  )
}


avro_set_schema <- function(options, avro_schema) {
  if (!is.null(avro_schema)) {
    if (!is.character(avro_schema)) {
      stop("Expect Avro schema to be a JSON string")
    }
    options$avroSchema <- avro_schema
  }
  options
}


#' Read Apache Avro data into a Spark DataFrame.
#'
#' Notice this functionality requires the Spark connection \code{sc} to be instantiated with either
#' an explicitly specified Spark version (i.e.,
#' \code{spark_connect(..., version = <version>, packages = c("avro", <other package(s)>), ...)})
#' or a specific version of Spark avro package to use (e.g.,
#' \code{spark_connect(..., packages = c("org.apache.spark:spark-avro_2.12:3.0.0", <other package(s)>), ...)}).
#' @inheritParams spark_read_csv
#' @param avro_schema Optional Avro schema in JSON format
#' @param ignore_extension If enabled, all files with and without .avro extension
#' are loaded (default: \code{TRUE})
#' @family Spark serialization routines
#' @export
spark_read_avro <- function(
  sc,
  name = NULL,
  path = name,
  avro_schema = NULL,
  ignore_extension = TRUE,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE
) {
  validate_spark_avro_pkg_version(sc)

  options <- avro_set_schema(list(), avro_schema)
  options$ignoreExtension <- ignore_extension

  spark_read_source(
    sc,
    name = name,
    path = path,
    source = "avro",
    options = options,
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
  )
}


#' Read binary data into a Spark DataFrame.
#'
#' Read binary files within a directory and convert each file into a record
#' within the resulting Spark dataframe. The output will be a Spark dataframe
#' with the following columns and possibly partition columns:
#'   \itemize{
#'     \item path: StringType
#'     \item modificationTime: TimestampType
#'     \item length: LongType
#'     \item content: BinaryType
#'  }
#'
#' @inheritParams spark_read_csv
#' @param dir Directory to read binary files from.
#' @param path_glob_filter Glob pattern of binary files to be loaded
#'   (e.g., "*.jpg").
#' @param recursive_file_lookup If FALSE (default), then partition discovery
#'   will be enabled (i.e., if a partition naming scheme is present, then
#'   partitions specified by subdirectory names such as "date=2019-07-01" will
#'   be created and files outside subdirectories following a partition naming
#'   scheme will be ignored). If TRUE, then all nested directories will be
#'   searched even if their names do not follow a partition naming scheme.
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_binary <- function(
  sc,
  name = NULL,
  dir = name,
  path_glob_filter = "*",
  recursive_file_lookup = FALSE,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE
) {
  if (spark_version(sc) < "3.0.0") {
    stop("Binary file data source is only supported in Spark 3.0 or above.")
  }

  spark_read_source(
    sc,
    name = name,
    path = dir,
    source = "binaryFile",
    options = list(
      pathGlobFilter = path_glob_filter,
      recursiveFileLookup = tolower(as.character(recursive_file_lookup))
    ),
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
  )
}


#' Read image data into a Spark DataFrame.
#'
#' Read image files within a directory and convert each file into a record
#' within the resulting Spark dataframe. The output will be a Spark dataframe
#' consisting of struct types containing the following attributes:
#'   \itemize{
#'     \item origin: StringType
#'     \item height: IntegerType
#'     \item width: IntegerType
#'     \item nChannels: IntegerType
#'     \item mode: IntegerType
#'     \item data: BinaryType
#'  }
#'
#' @inheritParams spark_read_csv
#' @param dir Directory to read binary files from.
#' @param drop_invalid Whether to drop files that are not valid images from the
#'   result (default: TRUE).
#'
#' @family Spark serialization routines
#'
#' @export
spark_read_image <- function(
  sc,
  name = NULL,
  dir = name,
  drop_invalid = TRUE,
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE
) {
  if (spark_version(sc) < "2.4.0") {
    stop("Image data source is only supported in Spark 2.4 or above.")
  }

  spark_read_source(
    sc,
    name = name,
    path = dir,
    source = "image",
    options = list(dropInvalid = drop_invalid),
    repartition = repartition,
    memory = memory,
    overwrite = overwrite
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
spark_read <- function(sc, paths, reader, columns, packages = TRUE, ...) {
  assert_that(is.function(reader) || is.language(reader))

  args <- list(...)
  paths <- lapply(paths, as.list)
  if (!identical(names(columns), NULL)) {
    # If columns is of the form c("col_name1", "col_name2", ...)
    # then leave it as-is
    # Otherwise if it is of the form c(col_name1 = "col_type1", ...)
    # or list(col_name1 = "col_type1", ...), etc, then make sure it gets coerced
    columns <- as.list(columns)
  } else {
    if (spark_version(sc) >= "4") {
      stop(
        "Only a named list with the name of the column and",
        " type of column is valid on Spark 4+"
      )
    }
  }

  rdd_base <- invoke_static(
    sc,
    "sparklyr.Utils",
    "createDataFrame",
    spark_context(sc),
    paths,
    as.integer(length(paths))
  )

  if (is.language(reader)) {
    f <- rlang::as_closure(reader)
  }
  serializer <- spark_apply_serializer()
  serialize_impl <- (if (is.list(serializer)) {
    serializer$serializer
  } else {
    serializer
  })
  deserializer <- spark_apply_deserializer()
  reader <- serialize_impl(reader)
  worker_impl <- function(df, rdr) {
    rdr <- deserializer(rdr)
    do.call(rbind, lapply(df$path, function(path) rdr(path)))
  }
  worker_impl <- serialize_impl(worker_impl)

  worker_port <- as.integer(
    spark_config_value(sc$config, "sparklyr.gateway.port", "8880")
  )

  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) {
    packages <- FALSE
  }

  bundle_path <- get_spark_apply_bundle_path(sc, packages)

  serialized_worker_context <- serialize_impl(
    list(column_types = list("character"), user_context = reader)
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
      args$profile,
      spark_read = TRUE
    ),
    worker_port,
    list("path"),
    list(),
    raw(),
    bundle_path,
    new.env(),
    as.integer(60),
    serialized_worker_context,
    new.env(),
    serialize(serializer, NULL),
    serialize(deserializer, NULL)
  )
  rdd <- invoke(rdd, "cache")
  schema <- spark_schema_from_rdd(sc, rdd, columns)
  sdf <- invoke(hive_context(sc), "createDataFrame", rdd, schema) %>%
    sdf_register()

  sdf
}

spark_csv_embedded_namespace <- function() {
  "com.databricks.spark.csv"
}


spark_csv_is_embedded <- function(sc) {
  invoke_static(
    sc,
    "sparklyr.Utils",
    "classExists",
    paste(spark_csv_embedded_namespace(), "CsvParser", sep = ".")
  )
}


spark_csv_is_loaded <- function(sc) {
  if (spark_version(sc) >= "2.0.0") {
    TRUE
  } else {
    spark_csv_is_embedded(sc)
  }
}


spark_csv_format_if_needed <- function(source, sc) {
  if (spark_csv_is_embedded(sc)) {
    invoke(source, "format", spark_csv_embedded_namespace())
  } else {
    source
  }
}


spark_csv_load_name <- function(sc) {
  if (spark_csv_is_embedded(sc)) "load" else "csv"
}


spark_csv_save_name <- function(sc) {
  if (spark_csv_is_embedded(sc)) "save" else "csv"
}


spark_csv_read <- function(sc, path, csvOptions = list(), columns = NULL) {
  read <- invoke(hive_context(sc), "read")

  options <- spark_csv_format_if_needed(read, sc)

  if (
    spark_config_value(sc$config, "sparklyr.verbose", FALSE) &&
      !identical(columns, NULL)
  ) {
    ncol_ds <- options %>%
      invoke(spark_csv_load_name(sc), path) %>%
      invoke("schema") %>%
      invoke("length")

    if (ncol_ds != length(columns)) {
      warning(
        "Dataset has ",
        ncol_ds,
        " columns but 'columns' has length ",
        length(columns)
      )
    }
  }

  for (csvOptionName in names(csvOptions)) {
    options <- invoke(
      options,
      "option",
      csvOptionName,
      csvOptions[[csvOptionName]]
    )
  }

  columnsHaveTypes <- !identical(columns, NULL) && length(names(columns)) > 0

  if (identical(columns, NULL) || !columnsHaveTypes) {
    optionSchema <- options
  } else {
    columnDefs <- spark_data_build_types(sc, columns)
    optionSchema <- invoke(options, "schema", columnDefs)
  }

  df <- invoke(
    optionSchema,
    spark_csv_load_name(sc),
    path
  )

  if (
    (identical(columns, NULL) && identical(csvOptions$header, "false")) ||
      (!identical(columns, NULL) && !columnsHaveTypes)
  ) {
    if (!identical(columns, NULL)) {
      newNames <- columns
    } else {
      # create normalized column names when header = FALSE and a columns specification is not supplied
      columns <- invoke(df, "columns")
      n <- length(columns)
      newNames <- sprintf("V%s", seq_len(n))
    }
    df <- invoke(df, "toDF", as.list(newNames))
  } else {
    # sanitize column names
    colNames <- as.character(invoke(df, "columns"))
    sanitized <- spark_sanitize_names(colNames, sc$config)
    if (!identical(colNames, sanitized)) {
      df <- invoke(df, "toDF", as.list(sanitized))
    }
  }

  df
}
