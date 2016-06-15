

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

#' Loads a CSV file and provides a data source compatible with dplyr
#'
#' Loads a CSV file and provides a data source compatible with dplyr
#'
#'
#' @param sc Connection to dplyr source
#' @param name Name to reference the data source once it's loaded
#' @param path The path to the file. Needs to be accessible from the cluster. Supports: "hdfs://" or "s3n://"
#' @param memory Loads data into memory
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite Overwrite the table with the given name when it exists
#'
#' @export
load_csv <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  con <- sc
  if (overwrite) spark_remove_table_if_exists(con, name)

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
#'
#' Saves a dplyr operation result as a CSV file
#'
#' @inheritParams load_csv
#' @param x A dplyr operation, for instance, `tbls(db, "flights")`
#'
#' @export
save_csv <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_save_csv(sqlResult, path)
}

#' Loads a parquet file and provides a data source compatible with dplyr
#'
#' Loads a parquet file and provides a data source compatible with dplyr
#'
#' @inheritParams load_csv
#'
#' @export
load_parquet <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  con <- sc
  if (overwrite) spark_remove_table_if_exists(con, name)

  api <- spark_api(con)
  df <- spark_api_read_generic(api, list(path), "parquet")
  spark_partition_register_df(con, df, api, name, repartition, memory)
}

#' Saves dplyr operation result as a parquet file
#'
#' Saves dplyr operation result as a parquet file
#'
#' @inheritParams save_csv
#'
#' @export
save_parquet <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "parquet")
}

#' Loads a JSON file and provides a data source compatible with dplyr
#'
#' Loads a JSON file and provides a data source compatible with dplyr
#'
#' @inheritParams load_csv
#'
#' @export
load_json <- function(sc, name, path, repartition = 0, memory = TRUE, overwrite = TRUE) {
  con <- sc
  if (overwrite) spark_remove_table_if_exists(con, name)

  api <- spark_api(con)
  df <- spark_api_read_generic(api, path, "json")
  spark_partition_register_df(con, df, api, name, repartition, memory)
}

#' Saves dplyr operation result as a JSON file
#'
#' Saves dplyr operation result as a JSON file
#'
#' @inheritParams save_csv
#'
#' @export
save_json <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path, "json")
}

#' Loads a dataframe and provides a data source compatible with dplyr
#'
#' Loads a dataframe and provides a data source compatible with dplyr
#'
#' @inheritParams load_csv
#' @param value R data frame to load into Spark
#'
#' @export
load_df <- function(sc, name, value, memory = TRUE, repartition = 0, overwrite = TRUE) {
  con <- sc
  if (overwrite) spark_remove_table_if_exists(con, name)

  dbWriteTable(con$con, name, value, TRUE, repartition)

  if (memory) {
    tbl_cache(con, name)
  }

  on_connection_updated(src_context(con), name)

  tbl(con, name)
}
