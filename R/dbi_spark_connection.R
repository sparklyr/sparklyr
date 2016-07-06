
# DBISparkConnection and methods.

setMethod("dbGetInfo", "DBISparkConnection", function(dbObj, ...) {
  dbObj$con@scon
})

setMethod("show", "DBISparkConnection", function(object) {
  info <- dbGetInfo(object)

  cat("<DBISparkConnection> ", info$master, "\n", sep = "")
})

# Connect to Spark
setMethod("dbConnect", "DBISparkDriver", function(drv, ...) {
  drv@scon$dbi
})

setMethod("dbDisconnect", "DBISparkConnection", function(conn, ...) {
  spark_disconnect(conn@scon)

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
