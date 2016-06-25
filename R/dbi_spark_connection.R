
# DBISparkConnection and methods.

#' DBI Spark Connection.
#' 
#' @slot scon A Spark connection.
#' @slot api Access to Spark API.
#' 
#' @keywords internal
#' 
#' @export
setClass("DBISparkConnection",
         contains = "DBIConnection",
         slots = c(scon = "sparklyr_connection",
                   api = "list")
)

setMethod("dbGetInfo", "DBISparkConnection", function(dbObj, ...) {
  dbObj$con@scon
})

setMethod("show", "DBISparkConnection", function(object) {
  info <- dbGetInfo(object)

  cat("<DBISparkConnection> ", info$master, "\n", sep = "")
})

# Connect to Spark
setMethod("dbConnect", "DBISparkDriver", function(drv, ...) {
  # get the inst and update it with a dbi reference
  sconInst <- spark_connection_get_inst(drv@scon)
  if (is.null(sconInst$dbi)) {
    api <- spark_api_create(drv@scon)

    # create the DBI connection
    dbi <- new("DBISparkConnection", scon = drv@scon, api = api)

    sconInst <- spark_connection_get_inst(drv@scon)
    sconInst$dbi <- dbi
    spark_connection_set_inst(drv@scon, sconInst)

    # Apply sql connection level properties
    params <- spark_config_params(drv@scon$config, spark_connection_is_local(drv@scon), "spark.sql.")
    lapply(names(params), function(paramName) {
      dbSetProperty(dbi, paramName, as.character(params[[paramName]]))
    })
  }

  # return dbi
  sconInst$dbi
})

setMethod("dbDisconnect", "DBISparkConnection", function(conn, ...) {
  stop_shell(conn@scon)

  invisible(TRUE)
})

# Determine database type for R vector.
setMethod("dbDataType", "DBISparkDriver", function(dbObj, obj) {
  get_data_type(obj)
})

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

setMethod("dbQuoteIdentifier", c("DBISparkConnection", "character"), function(conn, x, ...) {
  if (regexpr("`", x)[[1]] >= 0)
    stop("Can't scape back tick from string")

  y <- paste("`", x, "`", sep = "")

  SQL(y)
})

# Sets a property for the connection
setGeneric("dbSetProperty", function(conn, property, value) standardGeneric("dbSetProperty"));

# Sets a property for the connection
setMethod("dbSetProperty", c("DBISparkConnection", "character", "character"), function(conn, property, value) {
  dbGetQuery(
    conn,
    paste(
      "SET",
      paste(property, value, sep = "=")
    )
  )
})
