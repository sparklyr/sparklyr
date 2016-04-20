#' Spark driver
#'
#' This driver never needs to be unloaded and hence \code{dbUnload()} is a
#' null-op.
#'
#' @export
#' @import methods DBI
#' @examples
#' library(DBI)
#' splyr::DBISpark()
DBISpark <- function() {
  new("DBIDriverSpark")
}

#' DBIDriverSpark and methods.
#'
#' @export
#' @keywords internal
setClass("DBIDriverSpark", contains = "DBIDriver")

#' @export
#' @rdname DBIDriverSpark-class
setMethod("dbUnloadDriver", "DBIDriverSpark", function(drv, ...) {
  NULL
})
