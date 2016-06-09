#' @name dplyr-spark-data
#' @param con Connection to dplyr source
#' @param name Name to reference the data source once it's loaded
#' @param x A dplyr operation, for instance, `tbls(db, "flights")`
#' @param path The path to the CSV file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Loads data into memory
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
NULL

spark_partition_register_df <- function(con, df, api, name, repartition, memory) {
  if (repartition > 0) {
    df <- spark_invoke(df, "repartition", as.integer(repartition))
  }

  spark_register_temp_table(api, df, name)

  if (memory) {
    dbGetQuery(con$con, paste("CACHE TABLE", dplyr::escape(ident(name), con = con$con)))
    dbGetQuery(con$con, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = con$con)))
  }

  tbl(con, name)
}

#' Loads a CSV file and provides a data source compatible with dplyr
#' @rdname dplyr-spark-data
#' @export
load_csv <- function(con, name, path, repartition = 0, memory = TRUE) {
  api <- spark_api(con)
  df <- spark_read_csv(api, path)
  spark_partition_register_df(con, df, api, name, repartition, memory)
}

spark_source_from_ops <- function(x) {
  classList <- lapply(x, function(e) { attr(e, "class") } )

  if (!all(lapply(classList, function(e) !("src" %in% e) || ("src_spark" %in% e)) == TRUE)) {
    stop("This operation does not support multiple remote sources")
  }

  Filter(function(e) "src_spark" %in% attr(e, "class") , x)[[1]]
}

spark_sqlresult_from_dplyr <- function(x) {
  sparkSource <- spark_source_from_ops(x)

  api <- spark_api(sparkSource)
  sql <- dplyr::sql_render(x)
  sqlResult <- spark_api_sql(api, as.character(sql))
}

#' Saves a dplyr operation result as a CSV file
#' @rdname dplyr-spark-data
#' @export
save_csv <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_save_csv(sqlResult, path)
}

#' Loads a parquet file and provides a data source compatible with dplyr
#' @rdname dplyr-spark-data
#' @export
load_parquet <- function(con, name, path, repartition = 0, memory = TRUE) {
  api <- spark_api(con)
  df <- spark_api_read_generic(api, list(path), "parquet")
  spark_partition_register_df(con, df, api, name, repartition, memory)
}

#' Saves dplyr operation result as a parquet file
#' @rdname dplyr-spark-data
#' @export
save_parquet <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "parquet")
}

#' Loads a JSON file and provides a data source compatible with dplyr
#' @rdname dplyr-spark-data
#' @export
load_json <- function(con, name, path, repartition = 0, memory = TRUE) {
  api <- spark_api(con)
  df <- spark_api_read_generic(api, path, "json")
  spark_partition_register_df(con, df, api, name, repartition, memory)
}

#' Saves dplyr operation result as a JSON file
#' @rdname dplyr-spark-data
#' @export
save_json <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "json")
}
