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
    if (spark_version(sc) < "2.0.0") {
      invoke(df, "registerTempTable", name)
    } else {
      invoke(df, "createOrReplaceTempView", name)
    }
  }

  if (memory) {
    dbSendQuery(sc, paste("CACHE TABLE", escape(ident(unname(name)), con = sc)))
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
  # dbplyr (>= 2.6.0) rebuilds the lazy table `src` using the connection's
  # class (e.g. `src_spark_connection`) rather than `src_spark`, so a Spark
  # source is identified by its underlying connection instead of a fixed class.
  is_spark_src <- function(e) {
    inherits(e, "src_spark") || inherits(e[["con"]], "spark_connection")
  }

  # Note: `x` is a `tbl_lazy`, whose `[` method is not list subsetting, so use
  # `purrr::map_lgl()`/`[[` (which iterate it as a list) rather than
  # `Filter()`/`Find()`.
  is_foreign_src <- purrr::map_lgl(
    x,
    ~ inherits(.x, "src") && !is_spark_src(.x)
  )
  if (any(is_foreign_src)) {
    stop("This operation does not support multiple remote sources")
  }

  is_spark <- purrr::map_lgl(x, ~ inherits(.x, "src") && is_spark_src(.x))
  x[[which(is_spark)[[1]]]]
}

#' @importFrom dbplyr sql_render
spark_sqlresult_from_dplyr <- function(x) {
  # Validate that all remote sources in the lazy ops are Spark sources.
  spark_source_from_ops(x)
  sc <- spark_connection(x)

  sql <- sql_render(x)
  sqlResult <- invoke(hive_context(sc), "sql", as.character(sql))
}
