#' @include dbi_spark_driver.R
NULL

#' DBISparkConnection and methods.
#'
#' @keywords internal
#' @export
#' @rdname dbi-spark-connection
setClass("DBISparkConnection",
         contains = "DBIConnection",
         slots = c(scon = "spark_connection",
                   api = "list")
)

#' @export
#' @rdname dbi-spark-connection
setMethod("dbGetInfo", "DBISparkConnection", function(dbObj, ...) {
  dbObj$con@scon
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
#' @import config
#' @rdname dbi-spark-connection
#' @examples
#' \dontrun{
#' sc <- spark_connect()
#' con <- dbConnect(spark::DBISpark(sc))
#' dbDisconnect(con)
#' }
setMethod("dbConnect", "DBISparkDriver", function(drv, ...) {
  api <- spark_api_create(drv@scon)

  # create the DBI connection
  dbi <- new("DBISparkConnection", scon = drv@scon, api = api)

  # get the inst and update it with a dbi reference
  sconInst <- spark_connection_get_inst(drv@scon)
  sconInst$dbi <- dbi
  spark_connection_set_inst(drv@scon, sconInst)

  # build connect call that includes DBI connection
  connectCall <- strsplit(sconInst$connectCall, "\n")[[1]]
  connectCall <- paste(connectCall[[1]],
                       connectCall[[2]],
                       "db <- dbConnect(DBISpark(sc))",
                       sep = "\n")

  # call connection opended
  on_connection_opened(drv@scon, connectCall)

  # Apply sql connection level properties
  params <- spark_config_params(drv@scon$config, drv@scon$isLocal, "spark.sql.")
  lapply(names(params), function(paramName) {
    dbSetProperty(dbi, paramName, as.character(params[[paramName]]))
  })

  # return dbi
  dbi
})

#' @export
#' @rdname dbi-spark-connection
setMethod("dbDisconnect", "DBISparkConnection", function(conn, ...) {
  stop_shell(conn@scon)

  invisible(TRUE)
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

#' Sets a property for the connection
#' @keywords internal
setGeneric("dbSetProperty", function(conn, property, value) standardGeneric("dbSetProperty"));

#' Sets a property for the connection
#' @export
#' @keywords internal
#' @param property The name of the property, for instance, "spark.sql.shuffle.partitions"
setMethod("dbSetProperty", c("DBISparkConnection", "character", "character"), function(conn, property, value) {
  dbGetQuery(
    conn,
    paste(
      "SET",
      paste(property, value, sep = "=")
    )
  )
})
