spark_csv_options <- function(header,
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
#' @param sc The Spark connection
#' @param name Name of table
#' @param path The path to the file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Load data eagerly into memory
#' @param header Should the first row of data be used as a header? Defaults to \code{TRUE}.
#' @param delimiter The character used to delimit each column, defaults to \code{,}.
#' @param quote The character used as a quote, defaults to \code{"hdfs://"}.
#' @param escape The chatacter used to escape other characters, defaults to \code{\\}.
#' @param charset The character set, defaults to \code{"UTF-8"}.
#' @param null_value The character to use for default values, defaults to \code{NULL}.
#' @param options A list of strings with additional options.
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite Overwrite the table with the given name if it already exists
#'
#' @details You can read data from HDFS (\code{hdfs://}), S3 (\code{s3n://}), as well as 
#'   the local file system (\code{file://}). 
#'   
#' If you are reading from a secure S3 bucket be sure that the \code{AWS_ACCESS_KEY_ID} and 
#'   \code{AWS_SECRET_ACCESS_KEY} environment variables are both defined.    
#'
#' @return Reference to a Spark DataFrame / dplyr tbl
#' 
#' @family reading and writing data
#' 
#' @export
spark_read_csv <- function(sc,
                           name,
                           path,
                           header = TRUE,
                           delimiter = ",",
                           quote = "\"",
                           escape = "\\",
                           charset = "UTF-8",
                           null_value = NULL,
                           options = list(),
                           repartition = 0,
                           memory = TRUE,
                           overwrite = TRUE) {
  
  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  
  options <- spark_csv_options(header, delimiter, quote, escape, charset, null_value, options)
  df <- spark_api_read_csv(api, path.expand(path), options)
  
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Write a Spark DataFrame to a CSV
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
#' 
#' @family reading and writing data
#' 
#' @export
spark_write_csv <- function(x, path,
                            header = TRUE,
                            delimiter = ",",
                            quote = "\"",
                            escape = "\\",
                            charset = "UTF-8",
                            null_value = NULL,
                            options = list()) {
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
                                      options = list()) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  options <- spark_csv_options(header, delimiter, quote, escape, charset, null_value, options)
  
  spark_api_write_csv(sqlResult, path.expand(path), options)
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
                                       options = list()) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  options <- spark_csv_options(header, delimiter, quote, escape, charset, null_value, options)
  
  spark_api_write_csv(x, path.expand(path), options)
}

#' Read a Parquet file into a Spark DataFrame
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
#' @family reading and writing data
#'
#' @export
spark_read_parquet <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_generic(api, list(path.expand(path)), "parquet")
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Write a Spark DataFrame to a Parquet file
#'
#' @inheritParams spark_write_csv
#'
#' @family reading and writing data
#'
#' @export
spark_write_parquet <- function(x, path) {
  UseMethod("spark_write_parquet")
}

#' @export
spark_write_parquet.tbl_spark <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path.expand(path), "parquet")
}

#' @export
spark_write_parquet.spark_jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_generic(x, path.expand(path), "parquet")
}

#' Read a JSON file into a Spark DataFrame
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
#' @family reading and writing data
#'
#' @export
spark_read_json <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  
  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_generic(api, path.expand(path), "json")
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Write a Spark DataFrame to a JSON file
#'
#' @inheritParams spark_write_csv
#'
#' @family reading and writing data
#'
#' @export
spark_write_json <- function(x, path) {
  UseMethod("spark_write_json")
}

#' @export
spark_write_json.tbl_spark <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path.expand(path), "json")
}

#' @export
spark_write_json.spark_jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_generic(x, path.expand(path), "json")
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