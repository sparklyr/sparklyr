

spark_partition_register_df <- function(con, df, api, name, repartition, memory) {
  if (repartition > 0) {
    df <- spark_invoke(df, "repartition", as.integer(repartition))
  }

  spark_register_temp_table(df, name)

  if (memory) {
    dbGetQuery(con$con, paste("CACHE TABLE", dplyr::escape(ident(name), con = con$con)))
    dbGetQuery(con$con, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = con$con)))
  }

  tbl(con, name)
}

spark_remove_table_if_exists <- function(con, name) {
  if (name %in% src_tbls(con)) {
    dbRemoveTable(con$con, name)
  }
}

#' Reads a CSV file and provides a data source compatible with dplyr
#'
#' Reads a CSV file and provides a data source compatible with dplyr
#'
#'
#' @param db dplyr interface
#' @param name Name to reference the data source once it's loaded
#' @param path The path to the file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Loads data into memory
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite Overwrite the table with the given name when it exists
#'
#' @export
spark_read_csv <- function(db, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(db, name)

  api <- spark_api(db)
  df <- spark_api_read_csv(api, path)
  spark_partition_register_df(db, df, api, name, repartition, memory)
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

#' Writes a dplyr operation result as a CSV file
#'
#' Writes a dplyr operation result as a CSV file
#'
#' @inheritParams spark_read_csv
#' @param x A dplyr operation, for instance, `tbls(db, "flights")`
#'
#' @export
spark_write_csv <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_csv(sqlResult, path)
}

#' Reads a parquet file and provides a data source compatible with dplyr
#'
#' Reads a parquet file and provides a data source compatible with dplyr
#'
#' @inheritParams spark_read_csv
#'
#' @export
spark_read_parquet <- function(db, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(db, name)

  api <- spark_api(db)
  df <- spark_api_read_generic(api, list(path), "parquet")
  spark_partition_register_df(db, df, api, name, repartition, memory)
}

#' Writes a dplyr operation result as a parquet file
#'
#' Writes a dplyr operation result as a parquet file
#'
#' @inheritParams spark_write_csv
#'
#' @export
spark_write_parquet <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "parquet")
}

#' Reads a JSON file and provides a data source compatible with dplyr
#'
#' REads a JSON file and provides a data source compatible with dplyr
#'
#' @inheritParams spark_read_csv
#'
#' @export
spark_read_json <- function(db, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(db, name)

  api <- spark_api(db)
  df <- spark_api_read_generic(api, path, "json")
  spark_partition_register_df(db, df, api, name, repartition, memory)
}

#' Writes a dplyr operation result as a JSON file
#'
#' Writes a dplyr operation result as a JSON file
#'
#' @inheritParams spark_write_csv
#'
#' @export
spark_write_json <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "json")
}

#' Loads a dataframe and provides a data source compatible with dplyr
#'
#' Loads a dataframe and provides a data source compatible with dplyr
#'
#' @inheritParams spark_read_csv
#' @param value R data frame to load into Spark
#'
#' @export
load_df <- function(db, name, value, memory = TRUE, repartition = 0, overwrite = TRUE) {

  if (overwrite) spark_remove_table_if_exists(db, name)

  dbWriteTable(db$con, name, value, TRUE, repartition)

  if (memory) {
    tbl_cache(db, name)
  }

  on_connection_updated(src_context(db), name)

  tbl(db, name)
}
