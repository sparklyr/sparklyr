#' DBI Spark Driver
#'
#' @export
#' @import methods DBI
#' @rdname dbi-spark-driver
#' @examples
#' library(DBI)
#' spark::DBISpark()
DBISpark <- function(master = "master", appName = "dbispark") {
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
