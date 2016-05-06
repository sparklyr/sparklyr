#' DBI Spark Driver
#'
#' @export
#' @import methods DBI
#' @rdname dbi-spark-driver
#' @examples
#' spark::DBISpark()
DBISpark <- function(master = "local", appName = "dbispark") {
  new("DBISparkDriver", master = master, appName = appName)
}

#' DBISparkDriver and methods.
#'
#' @export
#' @keywords internal
#' @rdname dbi-spark-driver
setClass("DBISparkDriver",
         contains = "DBIDriver",
         slots = c(
           master = "character",
           appName = "character"))

#' @export
#' @rdname dbi-spark-driver
setMethod("dbUnloadDriver", "DBISparkDriver", function(drv, ...) {
  TRUE
})
