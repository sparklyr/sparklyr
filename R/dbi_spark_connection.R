#' @include dbi_spark_driver.R
NULL

#' DBISparkConnection and methods.
#'
#' @keywords internal
#' @export
#' @rdname dbi-spark-connection
setClass("DBISparkConnection",
         contains = "DBIConnection"
)

#' @export
#' @rdname dbi-spark-connection
setMethod("dbGetInfo", "DBISparkConnection", function(dbObj, ...) {
  connection_info(dbObj)
})

#' @export
#' @rdname dbi-spark-connection
setMethod("show", "DBISparkConnection", function(object) {
  info <- dbGetInfo(object)

  cat("<DBISparkConnection> ", info$master, "\n", sep = "")
})

#' Connect to Spark
#'
#' @param drv \code{splyr::DBISpark()}
#' @param master Master location.
#' @export
#' @rdname dbi-spark-connection
#' @examples
#' library(DBI)
#' con <- dbConnect(splyr::DBISpark())
#' dbDisconnect(con)
setMethod("dbConnect", "DBISparkConnection", function(drv, master = NULL, ...) {
  new("DBISparkConnection")
})

#' @export
#' @rdname dbi-spark-connection
setMethod("dbDisconnect", "DBISparkConnection", function(conn, ...) {
  TRUE
})

#' Determine database type for R vector.
#'
#' @export
#' @param dbObj Spark driver or connection.
#' @param obj Object to convert
#' @keywords internal
#' @rdname dbi-spark-connection
setMethod("dbDataType", "DBISparkDriver", function(dbObj, obj) {
  get_data_type(obj)
})

#' @export
#' @rdname dbi-spark-connection
setMethod("dbDataType", "DBISparkConnection", function(dbObj, obj) {
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
