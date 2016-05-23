#' @name dplyr-spark-data
#' @param con Connection to dplyr source
#' @param name Name to reference the data source once it's loaded
#' @param x A dplyr operation, for instance, `tbls(db, "flights")`
#' @param path The path to the CSV file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
NULL

#' Loads a CSV file and provides a data source compatible with dplyr
#' @rdname dplyr-spark-data
#' @export
load_csv <- function(con, name, path, repartition = 0) {
  api <- spark_api(con)
  df <- spark_read_csv(api, path)

  if (repartition > 0) {
    df <- spark_invoke(df, "repartition", as.integer(repartition))
  }

  spark_register_temp_table(api, df, name)
  tbl(con, name)
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

load_parquet <- function() {
}

save_parquet <- function() {
}

#' Loads a JSON file and provides a data source compatible with dplyr
#' @rdname dplyr-spark-data
#' @export
load_json <- function(con, name, path, repartition = 0) {
  api <- spark_api(con)
  df <- spark_read_json(api, path)

  if (repartition > 0) {
    df <- spark_invoke(df, "repartition", as.integer(repartition))
  }

  spark_register_temp_table(api, df, name)
  tbl(con, name)
}

#' Saves dplyr operation result as a JSON file
#' @rdname dplyr-spark-data
#' @export
save_json <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_save_json(sqlResult, path)
}
