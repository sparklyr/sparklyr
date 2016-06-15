#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @import parallel
#' @export
#' @param scon Spark connection provided by spark_connection
src_spark <- function(scon) {
  if (missing(scon))
    stop("Need to specify an Spark connection created. See spark_connection.")

  dbiCon <- dbConnect(DBISpark(scon))
  db <- src_sql("spark", dbiCon)

  # call connection opened with revised connectCall
  sconInst <- spark_connection_get_inst(scon)
  connectCall <- strsplit(sconInst$connectCall, "\n")[[1]]
  connectCall <- paste(connectCall[[1]],
                       "library(dplyr)",
                       connectCall[[2]],
                       "db <- src_spark(sc)",
                       sep = "\n")
  on_connection_opened(scon, connectCall)

  db
}

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
db_data_type.src_spark <- function(...) {
}

#' Copies the source data frame into a Spark table
#' @export
#' @param con Connection to dplyr source
#' @param df Data frame to copy from
#' @param name Name of the destination table
#' @param cache Cache table into memory for improved performance
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite When TRUE, overwrites table with existing name
copy_to.src_spark <- function(con, df, name, cache = TRUE, repartition = 0, overwrite = TRUE) {
  result <- load_df(con, name, df, memory = cache, repartition = repartition, overwrite = overwrite)

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
partition <- function(.data, ..., seed = sample(.Machine$integer.max, 1)) {
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
