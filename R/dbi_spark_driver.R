#' DBI Spark Driver
#'
#' @export
#' @import methods DBI
#' @rdname dbi-spark-driver
#' @examples
#' \dontrun{
#' sc <- spark_connect("local")
#' spark::DBISpark(scon)
#' }
DBISpark <- function(sc) {
  new("DBISparkDriver", scon = sc)
}

#' DBISparkDriver and methods.
#'
#' @export
#' @keywords internal
#' @rdname dbi-spark-driver
setClass("DBISparkDriver",
         contains = "DBIDriver",
         slots = c(
           scon = "spark_connection")
        )

#' @export
#' @rdname dbi-spark-driver
setMethod("dbUnloadDriver", "DBISparkDriver", function(drv, ...) {
  invisible(TRUE)
})
