
setMethod("dbBegin", "spark_connection", function(conn) {
  TRUE
})

setMethod("dbCommit", "spark_connection", function(conn) {
  TRUE
})

setMethod("dbRollback", "spark_connection", function(conn) {
  TRUE
})
