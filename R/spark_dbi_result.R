#' DBIResultSpark results.
#'
#' @keywords internal
#' @include spark_dbi_connection.R
#' @export
setClass("DBIResultSpark",
         contains = "DBIResult",
         slots = list(
           sql = "character"
         )
)

#' @export
setMethod("dbGetStatement", "DBIResultSpark", function(res, ...) {
  res@sql
})

#' @export
setMethod("dbIsValid", "DBIResultSpark", function(dbObj, ...) {
  result_active(dbObj@ptr)
})

#' @export
setMethod("dbGetRowCount", "DBIResultSpark", function(res, ...) {
  result_rows_fetched(res@ptr)
})

#' @export
setMethod("dbGetRowsAffected", "DBIResultSpark", function(res, ...) {
  result_rows_affected(res@ptr)
})

#' @export
setMethod("dbColumnInfo", "DBIResultSpark", function(res, ...) {
  result_column_info(res@ptr)
})

#' Execute a SQL statement on a database connection
#'
#' To retrieve results a chunk at a time, use \code{dbSendQuery},
#' \code{dbFetch}, then \code{ClearResult}. Alternatively, if you want all the
#' results (and they'll fit in memory) use \code{dbGetQuery} which sends,
#' fetches and clears for you.
#'
#' @param conn A \code{\linkS4class{DBIConnectionSpark}} created by \code{dbConnect}.
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
#' @name postgres-query
NULL

#' @export
setMethod("dbSendQuery", c("DBIConnectionSpark", "character"), function(conn, statement, params = NULL, ...) {
  statement <- enc2utf8(statement)

  rs <- new("PqResult",
            ptr = result_create(conn@ptr, statement),
            sql = statement)

  if (!is.null(params)) {
    dbBind(rs, params)
  }

  rs
})

#' @param res Code a \linkS4class{DBIResultSpark} produced by
#'   \code{\link[DBI]{dbSendQuery}}.
#' @param n Number of rows to return. If less than zero returns all rows.
#' @inheritParams DBI::sqlRownamesToColumn
#' @export
setMethod("dbFetch", "DBIResultSpark", function(res, n = -1, ..., row.names = NA) {
  sqlColumnToRownames(result_fetch(res@ptr, n = n), row.names)
})

#' @export
setMethod("dbBind", "DBIResultSpark", function(res, params, ...) {
  params <- lapply(params, as.character)
  result_bind_params(res@ptr, params)
  invisible(res)
})

#' @export
setMethod("dbHasCompleted", "DBIResultSpark", function(res, ...) {
  result_active(res@ptr)
})

#' @export
setMethod("dbClearResult", "DBIResultSpark", function(res, ...) {
  result_release(res@ptr)
  TRUE
})
