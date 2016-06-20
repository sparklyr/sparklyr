
setMethod("dbBegin", "DBISparkConnection", function(conn) {
  TRUE
})

setMethod("dbCommit", "DBISparkConnection", function(conn) {
  TRUE
})

setMethod("dbRollback", "DBISparkConnection", function(conn) {
  TRUE
})
