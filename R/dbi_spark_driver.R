# '@import methods DBI
NULL

# DBI Spark Driver
DBISpark <- function(sc) {
  new("DBISparkDriver", scon = sc)
}

# DBISparkDriver and methods.
#' DBI Spark Driver.
#' 
#' @slot scon A Spark connection.
#' 
#' @export
setClass("DBISparkDriver",
         contains = "DBIDriver",
         slots = c(
           scon = "sparklyr_connection")
        )

setMethod("dbUnloadDriver", "DBISparkDriver", function(drv, ...) {
  invisible(TRUE)
})
