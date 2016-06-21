#' Reads a CSV file and provides a data source compatible with dplyr
#'
#' @param sc The Spark connection
#' @param name Name to reference the data source once it's loaded
#' @param path The path to the file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Loads data into memory
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite Overwrite the table with the given name when it exists
#'
#' @export
spark_read_csv <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  
  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_csv(api, path.expand(path))
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Writes a dplyr operation result as a CSV file
#'
#' @inheritParams spark_read_csv
#' @param x A dplyr operation, for instance, `tbls(db, "flights")`
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
spark_write_csv.jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_csv(x, path.expand(path))
}

#' Reads a parquet file and provides a data source compatible with dplyr
#'
#' @inheritParams spark_read_csv
#'
#' @export
spark_read_parquet <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_generic(api, list(path.expand(path)), "parquet")
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Writes a dplyr operation result as a parquet file
#'
#' @inheritParams spark_write_csv
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
spark_write_parquet.jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_generic(x, path.expand(path), "parquet")
}

#' Reads a JSON file and provides a data source compatible with dplyr
#'
#' @inheritParams spark_read_csv
#'
#' @export
spark_read_json <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  
  if (overwrite) spark_remove_table_if_exists(sc, name)
  
  api <- spark_api(sc)
  df <- spark_api_read_generic(api, path.expand(path), "json")
  spark_partition_register_df(sc, df, api, name, repartition, memory)
}

#' Writes a dplyr operation result as a JSON file
#'
#' @inheritParams spark_write_csv
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
spark_write_json.jobj <- function(x, path) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_api_write_generic(x, path.expand(path), "json")
}

spark_expect_jobj_class <- function(jobj, expectedClassName) {
  class <- spark_invoke(jobj, "getClass")
  className <- spark_invoke(class, "getName")
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