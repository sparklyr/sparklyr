#' DBISparkResult results.
#'
#' @keywords internal
#' @include dbi_spark_connection.R
#' @export
#' @rdname dbi-spark-query
setClass("DBISparkResult",
         contains = "DBIResult",
         slots = list(
           sql = "character",
           df = "data.frame"
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
  TRUE
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbGetRowCount", "DBISparkResult", function(res, ...) {
  nrow(res@df)
})

#' @export
setMethod("dbGetRowsAffected", "DBISparkResult", function(res, ...) {
  nrow(res@df)
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbColumnInfo", "DBISparkResult", function(res, ...) {
  ""
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
#' library(nycflights13)
#' db <- dbConnect(splyr::DBISpark())
#' dbWriteTable(db, "flights", flights, temporary = TRUE)
#'
#' # Run query to get results as dataframe
#' dbGetQuery(db, "SELECT * FROM flights LIMIT 1")
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
  df <- spark_api_sql_query(conn@con, statement)

  rs <- new("DBISparkResult",
            df = df,
            sql = statement)
  rs
})

#' @param res Code a \linkS4class{DBISparkResult} produced by
#'   \code{\link[DBI]{dbSendQuery}}.
#' @param n Number of rows to return. If less than zero returns all rows.
#' @inheritParams DBI::sqlRownamesToColumn
#' @export
#' @rdname dbi-spark-query
setMethod("dbFetch", "DBISparkResult", function(res, n = -1, ..., row.names = NA) {
  if (n == -1)
    res@df
  else
    res@df[n, ]
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbBind", "DBISparkResult", function(res, params, ...) {
  TRUE
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbHasCompleted", "DBISparkResult", function(res, ...) {
  TRUE
})

#' @export
#' @rdname dbi-spark-query
setMethod("dbClearResult", "DBISparkResult", function(res, ...) {
  TRUE
})
