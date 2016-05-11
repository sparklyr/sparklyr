#' @include dbi_spark_driver.R
NULL

#' DBISparkConnection and methods.
#'
#' @keywords internal
#' @export
#' @rdname dbi-spark-connection
setClass("DBISparkConnection",
         contains = "DBIConnection",
         slots = c(scon = "list",
                   api = "list")
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
#' @param drv \code{spark::DBISpark()}
#' @param master Master location.
#' @export
#' @rdname dbi-spark-connection
#' @examples
#' \dontrun{
#' sc <- spark_connect()
#' con <- dbConnect(spark::DBISpark(sc))
#' dbDisconnect(con)
#' }
setMethod("dbConnect", "DBISparkDriver", function(drv, ...) {
  api <- spark_api_create(drv@scon)

  new("DBISparkConnection", scon = drv@scon, api = api)
})

#' @export
#' @rdname dbi-spark-connection
setMethod("dbDisconnect", "DBISparkConnection", function(conn, ...) {
  stop_shell(conn@scon)

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

#' @export
#' @rdname dbi-spark-connection
setMethod("dbQuoteIdentifier", c("DBISparkConnection", "character"), function(conn, x, ...) {
  if (regexpr("`", x)[[1]] >= 0)
    stop("Can't scape back tick from string")

  y <- paste("`", x, "`", sep = "")

  SQL(y)
})
