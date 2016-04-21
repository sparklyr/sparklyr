#' @include spark_dbi_driver.R
NULL

#' DBIConnectionSpark and methods.
#'
#' @keywords internal
#' @export
setClass("DBIConnectionSpark",
         contains = "DBIConnection"
)

#' @export
setMethod("dbGetInfo", "DBIConnectionSpark", function(dbObj, ...) {
  connection_info(dbObj)
})

#' @export
setMethod("show", "DBIConnectionSpark", function(object) {
  info <- dbGetInfo(object)

  cat("<DBIConnectionSpark> ", info$master, "\n", sep = "")
})

#' Connect to Spark
#'
#' @param drv \code{splyr::DBISpark()}
#' @param master Master location.
#' @export
#' @examples
#' library(DBI)
#' con <- dbConnect(splyr::DBISpark())
#' dbDisconnect(con)
setMethod("dbConnect", "DBIConnectionSpark", function(drv, master = NULL, ...) {
  new("DBIConnectionSpark")
})

#' @export
setMethod("dbDisconnect", "DBIConnectionSpark", function(conn, ...) {
  TRUE
})

#' Determine database type for R vector.
#'
#' @export
#' @param dbObj Spark driver or connection.
#' @param obj Object to convert
#' @keywords internal
setMethod("dbDataType", "DBIDriverSpark", function(dbObj, obj) {
  get_data_type(obj)
})

#' @export
setMethod("dbDataType", "DBIConnectionSpark", function(dbObj, obj) {
  get_data_type(obj)
})

get_data_type <- function(obj) {
  if (is.factor(obj)) return("TEXT")

  switch(typeof(obj),
         integer = "INTEGER",
         double = "REAL",
         character = "STRING",
         logical = "INTEGER",
         list = "BLOB",
         stop("Unsupported type", call. = FALSE)
  )
}
