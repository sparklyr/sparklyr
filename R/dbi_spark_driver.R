#' DBI Spark Driver
#'
#' @export
#' @import methods DBI
#' @rdname dbi-spark-driver
#' @examples
#' spark::DBISpark()
DBISpark <- function(scon) {
  new("DBISparkDriver", scon = scon)
}

#' DBISparkDriver and methods.
#'
#' @export
#' @keywords internal
#' @rdname dbi-spark-driver
setClass("DBISparkDriver",
         contains = "DBIDriver",
         slots = c(
           scon = "list")
        )

#' @export
#' @rdname dbi-spark-driver
setMethod("dbUnloadDriver", "DBISparkDriver", function(drv, ...) {
  TRUE
})
