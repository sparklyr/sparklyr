#' Spark Transactions.
#'
#' \code{dbBegin}, \code{dbCommit} and \code{dbRollback}
#' are currently not supported and are implementing as no-ops.
#'
#' @name spark-transactions
#' @param conn DBI Spark connection
NULL

#' @export
#' @rdname spark-transactions
setMethod("dbBegin", "DBISparkConnection", function(conn) {
  TRUE
})

#' @export
#' @rdname spark-transactions
setMethod("dbCommit", "DBISparkConnection", function(conn) {
  TRUE
})

#' @export
#' @rdname spark-transactions
setMethod("dbRollback", "DBISparkConnection", function(conn) {
  TRUE
})
