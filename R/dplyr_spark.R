#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @import parallel
#' @export
#' @param scon Spark connection provided by spark_connection
src_spark <- function(scon) {
  dbiCon <- dbConnect(DBISpark(scon))
  db <- src_sql("spark", dbiCon)

  if (spark_connection_is_local(scon)) {
    cores <- spark_connection_cores(scon)
    cores <- if (identical(cores, NULL)) parallel::detectCores() else cores
    if (cores > 0) {
      dbSetProperty(dbiCon, "spark.sql.shuffle.partitions", as.character(cores))
    }
  }

  db
}

spark_dbi <- function(con) {
  con$con
}

spark_api <- function(con) {
  spark_dbi(con)@api
}

spark_scon <- function(con) {
  spark_api(con)$scon
}

#' @export
src_desc.src_spark <- function(db) {
  scon <- src_context(db)
  paste("spark connection",
        paste("master", spark_connection_master(scon), sep = "="),
        paste("app", spark_connection_app_name(scon), sep = "="),
        paste("local", spark_connection_is_local(scon), sep = "="))
}

#' Retrieves the Spark connection object from a given dplyr src
#' @export
#' @param db Spark dplyr source provided by src_spark
src_context <- function(db) {
  db$con@scon
}

#' @export
db_explain.src_spark <- function(con) {
  ""
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
db_data_type.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_begin.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_commit.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_rollback.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_create_table.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_insert_into.src_spark <- function(...) {
}

#' Removes a Spark table
#' @export
#' @param con Connection to dplyr source
#' @param name Name of the table to remove
sql_drop_table.src_spark <- function(con, name) {
  dbRemoveTable(con, name)
}

#' Copies the source data frame into a Spark table
#' @export
#' @param con Connection to dplyr source
#' @param df Data frame to copy from
#' @param name Name of the destination table
#' @param cache Cache table for improved performance
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
copy_to.src_spark <- function(con, df, name, cache = TRUE, repartition = 0) {
  result <- dbWriteTable(con$con, name, df, TRUE, repartition)

  if (cache) {
    tbl_cache(con, name)
  }

  on_connection_updated(src_context(con))

  invisible(result)
}

#' Loads a table into memory
#' @export
#' @param con Connection to dplyr source
#' @param name Name of the destination table
#' @param force Forces data to be loaded in memory by executing a count(*) over the table
tbl_cache <- function(con, name, force = TRUE) {
  dbGetQuery(con$con, paste("CACHE TABLE", dplyr::escape(ident(name), con = con$con)))

  if (force) {
    dbGetQuery(con$con, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = con$con)))
  }
}

#' Unloads table from memory
#' @export
#' @param con Connection to dplyr source
#' @param name Name of the destination table
tbl_uncache <- function(con, name) {
  dbGetQuery(con$con, paste("UNCACHE TABLE", dplyr::escape(ident(name), con = con$con)))
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_create_index.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_analyze.src_spark <- function(...) {
}

#' Prints information associated to the dplyr source
#' @export
#' @param x Reference to dplyr source
#' @param ... Additional parameters
print.src_spark <- function(x, ...) {
  cat(src_desc(db))
  cat("\n\n")

  spark_log(x$con@scon)
}

