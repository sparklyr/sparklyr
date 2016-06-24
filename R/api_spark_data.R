#' Read a CSV file into a Spark DataFrame
#'
#' @param sc The Spark connection
#' @param name Name of table
#' @param path The path to the file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Load data eagerly into memory
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
spark_read_csv <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  
  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_csv(api, path.expand(path))
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Write a Spark DataFrame to a CSV
#'
#' @inheritParams spark_read_csv
#' @param x A Spark DataFrame or dplyr operation
#' 
#' @family reading and writing data
#' 
#' @export
spark_write_csv <- function(x, path) {
  UseMethod("spark_write_csv")
}

#' @export
spark_write_csv.tbl_spark <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_csv(sqlResult, path.expand(path))
}

#' @export
spark_write_csv.sparkapi_jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_csv(x, path.expand(path))
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
spark_write_parquet.sparkapi_jobj <- function(x, path) {
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
spark_write_json.sparkapi_jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_generic(x, path.expand(path), "json")
}

spark_expect_jobj_class <- function(jobj, expectedClassName) {
  class <- sparkapi_invoke(jobj, "getClass")
  className <- sparkapi_invoke(class, "getName")
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