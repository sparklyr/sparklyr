#' @import dplyr

spark_dbi <- function(x, ...) {
  UseMethod("spark_dbi", x)
}

spark_dbi.src_spark <- function(x) {
  x$con
}

spark_dbi.sparklyr_connection <- function(x) {
  dbConnect(DBISpark(x))
}

spark_api <- function(x) {
  spark_dbi(x)@api
}


#' @export
sparkapi_connection.tbl_spark <- function(x, ...) {
  sparkapi_connection(x$src)
}

#' @export
sparkapi_connection.src_spark <- function(x, ...) {
  spark_dbi(x)@scon
}


#' @export
src_desc.src_spark <- function(x) {
  scon <- src_context(x)
  paste("spark connection",
        paste("master", spark_connection_master(scon), sep = "="),
        paste("app", spark_connection_app_name(scon), sep = "="),
        paste("local", spark_connection_is_local(scon), sep = "="))
}

src_context <- function(db) {
  db$con@scon
}

#' @export
db_explain.src_spark <- function(con, sql, ...) {
  ""
}

#' @export
tbl_vars.sparkapi_jobj <- function(x) {
  as.character(sparkapi_invoke(x, "columns"))
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
tbl.sparklyr_connection <- function(src, from, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(src)))
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.sparklyr_connection <- function(x, ...) {
  
  sconInst <- spark_connection_get_inst(x)
  
  ctx <- if (!is.null(sconInst$hive))
    sconInst$hive
  else
    sconInst$sql
  
  tbls <- sparkapi_invoke(ctx, "sql", "SHOW TABLES")
  tableNames <- spark_dataframe_read_column(tbls, "tableName")
  
  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy a local R data frame to Spark
#'
#' @param dest A Spark connection
#' @param df Local data frame to copy
#' @param name Name of the destination table
#' @param memory Cache table into memory 
#' @param repartition Partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite When TRUE, overwrites table with existing name
#' @param local_file When TRUE, uses a local file to copy the data frame, this is only available in local installs.
#' @param ... Unused
#' 
#' @return dplyr compatible reference to table
#'
#' @name copy_to
#'
#' @family dplyr
#' 
#' @export
copy_to.sparklyr_connection <- function(dest, df, name = deparse(substitute(df)),
                                        memory = TRUE, repartition = 0, overwrite = FALSE, local_file = NULL, ...) {
  sc <- dest
  dest <- src_sql("spark", dbConnect(DBISpark(sc)))

  if (overwrite)
    spark_remove_table_if_exists(dest, name)
  if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dbWriteTable(dest$con, name, df, TRUE, repartition, local_file)

  if (memory) {
    tbl_cache(sc, name)
  }

  on_connection_updated(sc, name)

  tbl(dest, name)
}

#' Load a table into memory
#' 
#' @param sc Spark connection
#' @param name Name of the destination table
#' @param force Forces data to be loaded in memory by executing a count(*) over the table
#' 
#' @family dplyr
#' 
#' @export
tbl_cache <- function(sc, name, force = TRUE) {
  dbiCon <- dbConnect(DBISpark(sc))

  dbGetQuery(dbiCon, paste("CACHE TABLE", dplyr::escape(ident(name), con = dbiCon)))

  if (force) {
    dbGetQuery(dbiCon, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = dbiCon)))
  }
}

#' Unload table from memory
#' 
#' @param sc Spark connection
#' @param name Name of the destination table
#' 
#' @family dplyr
#' 
#' @export
tbl_uncache <- function(sc, name) {
  dbiCon <- dbConnect(DBISpark(sc))
  dbGetQuery(dbiCon, paste("UNCACHE TABLE", dplyr::escape(ident(name), con = dbiCon)))
}

#' @export
print.src_spark <- function(x, ...) {
  cat(src_desc(x))
  cat("\n\n")

  spark_log(x$con@scon)
}

#' @export
db_save_query.DBISparkConnection <- function (con, sql, name, temporary = TRUE, ...) 
{
  df <- sparkapi_dataframe(con@scon, sql)
  sparkapi_invoke(df, "registerTempTable", name)
}