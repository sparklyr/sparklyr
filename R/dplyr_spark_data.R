
#' @importFrom dbplyr escape
#' @importFrom dbplyr ident
#' @importFrom dplyr tbl
#' @importFrom DBI dbListTables
#' @importFrom DBI dbSendQuery
#' @importFrom DBI dbGetQuery
spark_partition_register_df <- function(sc, df, name, repartition, memory) {
  if (repartition > 0) {
    df <- invoke(df, "repartition", as.integer(repartition))
  }

  if (!name %in% dbListTables(sc)) {
    if (spark_version(sc) < "2.0.0")
      invoke(df, "registerTempTable", name)
    else
      invoke(df, "createOrReplaceTempView", name)
  }

  if (memory) {
    dbSendQuery(sc, paste("CACHE TABLE", escape(ident(name), con = sc)))
    dbGetQuery(sc, paste("SELECT count(*) FROM", escape(ident(name), con = sc)))
  }

  on_connection_updated(sc, name)

  tbl(sc, name)
}

spark_remove_table_if_exists <- function(sc, name) {
  if (name %in% src_tbls(sc)) {
    dbRemoveTable(sc, name)
  }
}

spark_source_from_ops <- function(x) {
  classList <- lapply(x, function(e) { attr(e, "class") } )

  if (!all(lapply(classList, function(e) !("src" %in% e) || ("src_spark" %in% e)) == TRUE)) {
    stop("This operation does not support multiple remote sources")
  }

  Filter(function(e) "src_spark" %in% attr(e, "class") , x)[[1]]
}

#' @importFrom dbplyr sql_render
spark_sqlresult_from_dplyr <- function(x) {
  sparkSource <- spark_source_from_ops(x)
  sc <- spark_connection(sparkSource)

  sql <- sql_render(x)
  sqlResult <- invoke(hive_context(sc), "sql", as.character(sql))
}
