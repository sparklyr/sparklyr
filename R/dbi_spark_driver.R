#' DBI Spark Driver
#'
#' @export
#' @import methods DBI
#' @rdname dbi-spark-driver
#' @examples
#' library(DBI)
#' spark::DBISpark()
DBISpark <- function() {
  new("DBISparkDriver")
}

#' DBISparkDriver and methods.
#'
#' @export
#' @keywords internal
#' @rdname dbi-spark-driver
setClass("DBISparkDriver", contains = "DBIDriver")

#' @export
#' @rdname dbi-spark-driver
setMethod("dbUnloadDriver", "DBISparkDriver", function(drv, ...) {
  TRUE
})
