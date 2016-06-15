
#' @export
#' @rdname DBI-interface
setMethod("dbBegin", "DBISparkConnection", function(conn) {
  TRUE
})

#' @export
#' @rdname DBI-interface
setMethod("dbCommit", "DBISparkConnection", function(conn) {
  TRUE
})

#' @export
#' @rdname DBI-interface
setMethod("dbRollback", "DBISparkConnection", function(conn) {
  TRUE
})
