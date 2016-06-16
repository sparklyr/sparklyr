#' @import dplyr
#' @import parallel

spark_dbi <- function(x, ...) {
  UseMethod("spark_dbi", x)
}

spark_dbi.src_spark <- function(x) {
  x$con
}

spark_dbi.spark_connection <- function(x) {
  dbConnect(DBISpark(x))
}

spark_api <- function(x) {
  spark_dbi(x)@api
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
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(src)))
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.spark_connection <- function(x, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(x)))
  sort(src_tbls(src, ...))
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' @param dest A Spark connection
#' @param name Name of the destination table
#' @param df Local data frame to copy
#' @param ... Unused
#'
#' @name copy_to
#'
#' @export
copy_to.spark_connection <- function(dest, df, name = deparse(substitute(df)), ...) {
  sc <- dest
  args <- list(...)
  overwrite <- if (is.null(args$overwrite)) FALSE else args$overwrite

  dest <- src_sql("spark", dbConnect(DBISpark(sc)))

  if (overwrite)
    spark_remove_table_if_exists(dest, name)
  if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dbWriteTable(dest$con, name, df)

  tbl_cache(sc, name)
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

  dbGetQuery(dbiCon, paste("CACHE TABLE", dplyr::escape(ident(name), con = dbiCon)))

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
#' Partition a Spark DataFrame into multiple groups. This routine is useful
#' for splitting a DataFrame into, for example, training and test datasets.
#'
#' @param x A \code{tbl_spark}.
#' @param ... Named parameters, mapping table names to weights.
#' @param seed Random seed to use for randomly partitioning the dataset. Set
#'   this if you want your partitioning to be reproducible on repeated runs.
#'
#' @return An \R \code{list} of \code{tbl_spark}s.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # randomly partition data into a 'training' and 'test'
#' # dataset, with 60% of the observations assigned to the
#' # 'training' dataset, and 40% assigned to the 'test' dataset
#' diamonds_tbl <- copy_to(sc, diamonds, "diamonds")
#' partitions <- diamonds_tbl %>%
#'   df_partition(training = 0.6, test = 0.4)
#' print(partitions)
#' }
df_partition <- function(x, ..., seed = sample(.Machine$integer.max, 1)) {
  weights <- list(...)
  nm <- names(weights)
  if (is.null(nm) || any(!nzchar(nm)))
    stop("all weights must be named")

  splat <- spark_dataframe_split(x, as.numeric(weights), seed = seed)
  names(splat) <- nm

  db <- x$src
  partitions <- lapply(seq_along(splat), function(i) {
    spark_invoke(splat[[i]], "registerTempTable", nm[[i]])
    tbl(db, nm[[i]])
  })

  names(partitions) <- nm
  partitions
}
