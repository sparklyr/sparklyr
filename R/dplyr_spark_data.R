

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
