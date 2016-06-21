

spark_partition_register_df <- function(sc, df, api, name, repartition, memory) {
  if (repartition > 0) {
    df <- spark_invoke(df, "repartition", as.integer(repartition))
  }

  dbi <- spark_dbi(sc)
  spark_register_temp_table(df, name)

  if (memory) {
    dbGetQuery(dbi, paste("CACHE TABLE", dplyr::escape(ident(name), con = dbi)))
    dbGetQuery(dbi, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = dbi)))
  }
  
  on_connection_updated(sc, name)

  tbl(sc, name)
}

spark_remove_table_if_exists <- function(sc, name) {
  if (name %in% src_tbls(sc)) {
    dbi <- spark_dbi(sc)
    dbRemoveTable(dbi, name)
  }
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

#' Reads a parquet file and provides a data source compatible with dplyr
#'
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
#' Writes a dplyr operation result as a parquet file
#'
#' @inheritParams spark_write_csv
#'
#' @export
spark_write_parquet <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path.expand(path), "parquet")
}

#' Reads a JSON file and provides a data source compatible with dplyr
#'
#' REads a JSON file and provides a data source compatible with dplyr
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
#' Writes a dplyr operation result as a JSON file
#'
#' @inheritParams spark_write_csv
#'
#' @export
spark_write_json <- function(x, path) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_api_write_generic(sqlResult, path.expand(path), "json")
}

