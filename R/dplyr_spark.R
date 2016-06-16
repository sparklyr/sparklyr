#' @import dplyr
#' @import parallel

spark_dbi <- function(con) {
  con$con
}

spark_api <- function(con) {
  spark_dbi(con)@api
}

spark_scon <- function(x, ...) {
  UseMethod("spark_scon")
}

#' @export
spark_scon.spark_connection <- function(x, ...) {
  x
}

#' @export
spark_scon.jobj <- function(x, ...) {
  x$scon
}

#' @export
spark_scon.tbl_spark <- function(x, ...) {
  spark_scon(x$src)
}

#' @export
spark_scon.src_spark <- function(x, ...) {
  spark_dbi(x)@scon
}


#' @export
src_desc.src_spark <- function(db) {
  scon <- src_context(db)
  paste("spark connection",
        paste("master", spark_connection_master(scon), sep = "="),
        paste("app", spark_connection_app_name(scon), sep = "="),
        paste("local", spark_connection_is_local(scon), sep = "="))
}

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

#' @export
tbl.spark_connection <- function(sc, from, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(sc)))
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.spark_connection <- function(sc, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(sc)))
  src_tbls("spark", src, ...)
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' @param sc The Spark connection
#' @param name Name of the destination table
#' @param df Local data frame to copy
#' @param memory Cache table into memory for improved performance
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite When TRUE, overwrites table with existing name
#' @param ... Unused
#'
#' @name copy_to
#'
#' @export
copy_to.spark_connection <- function(sc, df, name = deparse(substitute(df)), ...,
                                     memory = TRUE, repartition = 0, overwrite = FALSE) {
  dest <- src_sql("spark", dbConnect(DBISpark(sc)))

  if (overwrite)
    spark_remove_table_if_exists(dest, name)
  else if (spark_table_exists(dest, name))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dbWriteTable(dest$con, name, df, TRUE, repartition)

  if (memory) {
    tbl_cache(dest, name)
  }

  on_connection_updated(src_context(dest), name)

  tbl(dest, name)
}

#' Loads a table into memory
#' @export
#' @param sc Connection to dplyr source
#' @param name Name of the destination table
#' @param force Forces data to be loaded in memory by executing a count(*) over the table
tbl_cache <- function(sc, name, force = TRUE) {
  dbiCon <- dbConnect(DBISpark(sc))

  dbGetQuery(dbiCon, paste("CACHE TABLE", dplyr::escape(ident(name), con = con$con)))

  if (force) {
    dbGetQuery(dbiCon, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = dbiCon)))
  }
}

#' Unloads table from memory
#' @export
#' @param sc Connection to dplyr source
#' @param name Name of the destination table
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

#' Partition a Spark Dataframe
#'
#' @param .data Data
#' @param ... Named parameters, mapping table names to weights.
#' @param seed Seed value for the partition
#' @export
ml_partition <- function(.data, ..., seed = sample(.Machine$integer.max, 1)) {
  weights <- list(...)
  nm <- names(weights)
  if (is.null(nm) || any(!nzchar(nm)))
    stop("all weights must be named")

  splat <- spark_dataframe_split(.data, as.numeric(weights), seed = seed)
  names(splat) <- nm

  db <- .data$src
  partitions <- lapply(seq_along(splat), function(i) {
    spark_invoke(splat[[i]], "registerTempTable", nm[[i]])
    tbl(db, nm[[i]])
  })

  names(partitions) <- nm
  partitions
}
