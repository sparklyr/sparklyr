#' DBISparkResult results.
#'
#' @keywords internal
#' @include dbi_spark_connection.R
#' @export
#' @rdname dbi-spark-query
setClass("DBISparkResult",
         contains = "DBIResult",
         slots = list(
           sql = "character"
         )
)

#' @export
#' @rdname dbi-spark-query
setMethod("dbGetStatement", "DBISparkResult", function(res, ...) {
  res@sql
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbIsValid", "DBISparkResult", function(dbObj, ...) {
  result_active(dbObj@ptr)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbGetRowCount", "DBISparkResult", function(res, ...) {
  result_rows_fetched(res@ptr)
})

#' @export
setMethod("dbGetRowsAffected", "DBISparkResult", function(res, ...) {
  result_rows_affected(res@ptr)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbColumnInfo", "DBISparkResult", function(res, ...) {
  result_column_info(res@ptr)
})

#' Execute a SQL statement on a database connection
#'
#' To retrieve results a chunk at a time, use \code{dbSendQuery},
#' \code{dbFetch}, then \code{ClearResult}. Alternatively, if you want all the
#' results (and they'll fit in memory) use \code{dbGetQuery} which sends,
#' fetches and clears for you.
#'
#' @param conn A \code{\linkS4class{DBISparkConnection}} created by \code{dbConnect}.
#' @param statement An SQL string to execture
#' @param params A list of query parameters to be substituted into
#'   a parameterised query.
#' @examples
#' library(DBI)
#' db <- dbConnect(splyr::DBISpark())
#' dbWriteTable(db, "usarrests", datasets::USArrests, temporary = TRUE)
#'
#' # Run query to get results as dataframe
#' dbGetQuery(db, "SELECT * FROM glights LIMIT 1")
#'
#' # Send query to pull requests in batches
#' res <- dbSendQuery(db, "SELECT * FROM usarrests")
#' dbFetch(res, n = 1)
#' dbHasCompleted(res)
#' dbClearResult(res)
#'
#' dbRemoveTable(db, "flights")
#'
#' dbDisconnect(db)
#' @name dbi-spark-query
NULL

#' @export
#' @rdname dbi-spark-query
setMethod("dbSendQuery", c("DBISparkConnection", "character"), function(conn, statement, params = NULL, ...) {
  statement <- enc2utf8(statement)

  rs <- new("PqResult",
            ptr = result_create(conn@ptr, statement),
            sql = statement)

  if (!is.null(params)) {
    dbBind(rs, params)
  }

  rs
})

#' @param res Code a \linkS4class{DBISparkResult} produced by
#'   \code{\link[DBI]{dbSendQuery}}.
#' @param n Number of rows to return. If less than zero returns all rows.
#' @inheritParams DBI::sqlRownamesToColumn
#' @export
#' @rdname dbi-spark-query
setMethod("dbFetch", "DBISparkResult", function(res, n = -1, ..., row.names = NA) {
  sqlColumnToRownames(result_fetch(res@ptr, n = n), row.names)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbBind", "DBISparkResult", function(res, params, ...) {
  params <- lapply(params, as.character)
  result_bind_params(res@ptr, params)
  invisible(res)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbHasCompleted", "DBISparkResult", function(res, ...) {
  result_active(res@ptr)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbClearResult", "DBISparkResult", function(res, ...) {
  result_release(res@ptr)
  TRUE
})
