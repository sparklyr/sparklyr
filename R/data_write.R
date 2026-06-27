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
#'   For more details see also \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes}
#'   for your version of Spark.
#' @param partition_by A \code{character} vector. Partitions the output by the given columns on the file system.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_csv <- function(
  x,
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
  ...
) {
  UseMethod("spark_write_csv")
}


#' @export
spark_write_csv.tbl_spark <- function(
  x,
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
  ...
) {
  df <- spark_writable_dataframe(x)
  options <- spark_csv_options(
    header,
    TRUE,
    delimiter,
    quote,
    escape,
    charset,
    null_value,
    options
  )

  spark_csv_write(
    df,
    spark_normalize_path(path),
    options,
    mode,
    partition_by
  )
}


#' @export
spark_write_csv.spark_jobj <- spark_write_csv.tbl_spark


#' Write a Spark DataFrame to a Parquet file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://parquet.apache.org/}{Parquet} format.
#'
#' @inheritParams spark_write_csv
#' @param options A list of strings with additional options. See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_parquet <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_parquet")
}


#' @export
spark_write_parquet.tbl_spark <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    spark_normalize_path(path),
    "parquet",
    mode,
    options,
    partition_by
  )
}


#' @export
spark_write_parquet.spark_jobj <- spark_write_parquet.tbl_spark


#' Write a Spark DataFrame to a JSON file
#'
#' Serialize a Spark DataFrame to the \href{https://www.json.org/}{JavaScript
#' Object Notation} format.
#'
#' @inheritParams spark_write_csv
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_json <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_json")
}


#' @export
spark_write_json.tbl_spark <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    spark_normalize_path(path),
    "json",
    mode,
    options,
    partition_by
  )
}


#' @export
spark_write_json.spark_jobj <- spark_write_json.tbl_spark


spark_expect_jobj_class <- function(jobj, expectedClassName) {
  className <- invoke(jobj, "%>%", list("getClass"), list("getName"))
  # A Spark DataFrame is `Dataset[Row]`; the concrete JVM class is
  # `org.apache.spark.sql.Dataset` on Spark 2.x/3.x and
  # `org.apache.spark.sql.classic.Dataset` on Spark 4.x. Compare on the simple
  # class name so the check is version-agnostic.
  simple_name <- function(x) sub(".*\\.", "", x)
  if (!simple_name(className) %in% simple_name(expectedClassName)) {
    stop(
      "This operation is only supported on ",
      expectedClassName,
      " jobjs but found ",
      className,
      " instead."
    )
  }
}


spark_writable_dataframe <- function(x) {
  if (inherits(x, "tbl_spark")) {
    spark_sqlresult_from_dplyr(x)
  } else {
    spark_expect_jobj_class(x, "org.apache.spark.sql.Dataset")
    x
  }
}


#' Writes a Spark DataFrame into a Spark table
#'
#' @inheritParams spark_write_csv
#' @param name The name to assign to the newly generated table.
#' @family Spark serialization routines
#'
#' @export
spark_write_table <- function(
  x,
  name,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
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
spark_write_table.tbl_spark <- function(
  x,
  name,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  df <- spark_writable_dataframe(x)

  fileMethod <- if (identical(mode, "append")) "insertInto" else "saveAsTable"

  spark_data_write_generic(
    df,
    name,
    fileMethod,
    mode,
    options,
    partition_by
  )
}


#' @export
spark_write_table.spark_jobj <- function(
  x,
  name,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    name,
    "saveAsTable",
    mode,
    options,
    partition_by
  )
}


#' Inserts a Spark DataFrame into a Spark table
#'
#' @inheritParams spark_write_csv
#' @inheritParams spark_read_csv
#' @param name The name to assign to the newly generated table.
#' @family Spark serialization routines
#' @export
spark_insert_table <- function(
  x,
  name,
  mode = NULL,
  overwrite = FALSE,
  options = list(),
  ...
) {
  UseMethod("spark_insert_table")
}


#' @export
spark_insert_table.tbl_spark <- function(
  x,
  name,
  mode = NULL,
  overwrite = FALSE,
  options = list(),
  ...
) {
  mode <- if (isTRUE(overwrite)) "overwrite" else "append"

  spark_data_write_generic(
    spark_writable_dataframe(x),
    name,
    "insertInto",
    mode,
    options,
    NULL
  )
}


#' @export
spark_insert_table.spark_jobj <- spark_insert_table.tbl_spark


#' Writes a Spark DataFrame into a JDBC table
#'
#' @inheritParams spark_write_csv
#' @param name The name to assign to the newly generated table.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(
#'   master = "local",
#'   config = list(
#'     `sparklyr.shell.driver-class-path` = "/usr/share/java/mysql-connector-java-8.0.25.jar"
#'   )
#' )
#' spark_write_jdbc(
#'   sdf_len(sc, 10),
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
#' @family Spark serialization routines
#'
#' @export
spark_write_jdbc <- function(
  x,
  name,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_jdbc")
}


#' @export
spark_write_jdbc.tbl_spark <- function(
  x,
  name,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    name,
    "jdbc",
    mode,
    options,
    partition_by,
    is_jdbc = TRUE
  )
}


#' @export
spark_write_jdbc.spark_jobj <- spark_write_jdbc.tbl_spark


#' Writes a Spark DataFrame into a generic source
#'
#' Writes a Spark DataFrame into a generic source.
#'
#' @inheritParams spark_write_csv
#' @param source A data source capable of reading data.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_source <- function(
  x,
  source,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_source")
}


#' @export
spark_write_source.tbl_spark <- function(
  x,
  source,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    source,
    "format",
    mode,
    options,
    partition_by,
    ...
  )
}


#' @export
spark_write_source.spark_jobj <- spark_write_source.tbl_spark


#' Write a Spark DataFrame to a Text file
#'
#' Serialize a Spark DataFrame to the plain text format.
#'
#' @inheritParams spark_write_csv
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_text <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_text")
}


#' @export
spark_write_text.tbl_spark <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    spark_normalize_path(path),
    "text",
    mode,
    options,
    partition_by
  )
}


#' @export
spark_write_text.spark_jobj <- spark_write_text.tbl_spark


#' Write a Spark DataFrame to a ORC file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://orc.apache.org/}{ORC} format.
#' @inheritParams spark_write_csv
#' @param options A list of strings with additional options.
#' See \url{https://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#' @family Spark serialization routines
#' @export
spark_write_orc <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_orc")
}


#' @export
spark_write_orc.tbl_spark <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  spark_data_write_generic(
    spark_writable_dataframe(x),
    spark_normalize_path(path),
    "orc",
    mode,
    options,
    partition_by
  )
}


#' @export
spark_write_orc.spark_jobj <- spark_write_orc.tbl_spark


#' Writes a Spark DataFrame into Delta Lake
#'
#' Writes a Spark DataFrame into Delta Lake.
#'
#' @inheritParams spark_write_csv
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_delta <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  UseMethod("spark_write_delta")
}

#' @export
spark_write_delta.tbl_spark <- function(
  x,
  path,
  mode = NULL,
  options = list(),
  partition_by = NULL,
  ...
) {
  options$path <- path
  spark_write_source(
    x,
    "delta",
    mode = mode,
    options = options,
    partition_by = partition_by
  )
}

#' @export
spark_write_delta.spark_jobj <- spark_write_delta.tbl_spark


#' Serialize a Spark DataFrame into Apache Avro format
#'
#' Notice this functionality requires the Spark connection \code{sc} to be
#' instantiated with either
#' an explicitly specified Spark version (i.e.,
#' \code{spark_connect(..., version = <version>, packages = c("avro", <other package(s)>), ...)})
#' or a specific version of Spark avro package to use (e.g.,
#' \code{spark_connect(..., packages =
#' c("org.apache.spark:spark-avro_2.12:3.0.0", <other package(s)>), ...)}).
#' @inheritParams spark_write_csv
#' @param avro_schema Optional Avro schema in JSON format
#' @param record_name Optional top level record name in write result
#' (default: "topLevelRecord")
#' @param record_namespace Record namespace in write result (default: "")
#' @param compression Compression codec to use (default: "snappy")
#' @family Spark serialization routines
#' @export
spark_write_avro <- function(
  x,
  path,
  avro_schema = NULL,
  record_name = "topLevelRecord",
  record_namespace = "",
  compression = "snappy",
  partition_by = NULL
) {
  validate_spark_avro_pkg_version(spark_connection(x))

  options <- avro_set_schema(list(), avro_schema)
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


#' Write Spark DataFrame to RDS files
#'
#' Write Spark dataframe to RDS files. Each partition of the dataframe will be
#' exported to a separate RDS file so that all partitions can be processed in
#' parallel.
#'
#' @param x A Spark DataFrame to be exported
#' @param dest_uri  Can be a URI template containing `partitionId` (e.g.,
#'   \code{"hdfs://my_data_part_{partitionId}.rds"}) where `partitionId` will be
#'   substituted with ID of each partition using `glue`, or a list of URIs
#'   to be assigned to RDS output from all partitions (e.g.,
#'   \code{"hdfs://my_data_part_0.rds"}, \code{"hdfs://my_data_part_1.rds"}, and so on)
#'   If working with a Spark instance running locally, then all URIs should be
#'   in \code{"file://<local file path>"} form. Otherwise the scheme of the URI should
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
  if (spark_version(sc) < "3.0.0") {
    stop("`spark_write_rds()` is only supported in Spark 3.0 or above")
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
    sc,
    "sparklyr.RDSCollector",
    "collect",
    spark_dataframe(x),
    as.list(dest_uri),
    spark_session(sc)
  )

  dplyr::tibble(
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
spark_write <- function(x, writer, paths, packages = NULL) {
  UseMethod("spark_write")
}


#' @export
spark_write.tbl_spark <- function(x, writer, paths, packages = NULL) {
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
spark_write.spark_jobj <- function(x, writer, paths, packages = NULL) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.Dataset")
  x %>%
    sdf_register() %>%
    spark_write(writer, paths, packages)
}


spark_csv_write <- function(df, path, csvOptions, mode, partition_by) {
  sc <- spark_connection(df)

  write <- invoke(df, "write")
  options <- spark_csv_format_if_needed(write, sc)

  for (csvOptionName in names(csvOptions)) {
    options <- invoke(
      options,
      "option",
      csvOptionName,
      csvOptions[[csvOptionName]]
    )
  }

  if (!is.null(partition_by)) {
    options <- invoke(options, "partitionBy", as.list(partition_by))
  }

  options <- spark_data_apply_mode(options, mode)

  invoke(
    options,
    spark_csv_save_name(sc),
    path
  )

  invisible(TRUE)
}
