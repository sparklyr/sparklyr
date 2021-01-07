# DBISparkConnection and methods.

setMethod("dbGetInfo", "spark_connection", function(dbObj, ...) {
  dbObj
})

setMethod("show", "spark_connection", function(object) {
  info <- dbGetInfo(object)

  cat("<spark_connection> ", info$master, "\n", sep = "")
})

# Determine database type for R vector.
setMethod("dbDataType", "spark_connection", function(dbObj, obj) {
  get_data_type(obj)
})

setMethod("sqlParseVariables", "spark_connection", function(conn, sql, ...) {
  method <- getMethod("sqlParseVariables", "DBIConnection")
  method(conn, sql, ...)
})

setMethod("sqlInterpolate", "spark_connection", function(conn, sql, ..., .dots = list()) {
  method <- getMethod("sqlInterpolate", "DBIConnection")
  method(conn, sql, ..., .dots = .dots)
})

setMethod("dbQuoteLiteral", "spark_connection", function(conn, x, ...) {
  method <- getMethod("dbQuoteLiteral", "DBIConnection")
  method(conn, x, ...)
})

get_data_type <- function(obj) {
  if (is.factor(obj)) {
    return("TEXT")
  }

  switch(typeof(obj),
    integer = "INTEGER",
    double = "REAL",
    character = "STRING",
    logical = "INTEGER",
    list = "BLOB",
    stop("Unsupported type", call. = FALSE)
  )
}

dbi_ensure_no_backtick <- function(x) {
  if (regexpr("`", x)[[1]] >= 0) stop("Can't escape back tick from string")
}

setMethod("dbQuoteIdentifier", c("spark_connection", "character"), function(conn, x, ...) {
  if (length(x) == 0L) {
    x
  } else {
    dbi_ensure_no_backtick(x)

    y <- paste("`", x, "`", sep = "")

    SQL(y)
  }
})

setMethod("dbQuoteString", c("spark_connection", "character"), function(conn, x, ...) {
  SQL(paste('"', gsub('"', '\\\\"', x), '"', sep = ""))
})

# Sets a property for the connection
setGeneric("dbSetProperty", function(conn, property, value) standardGeneric("dbSetProperty"))
# Sets a property for the connection
setMethod("dbSetProperty", c("spark_connection", "character", "character"), function(conn, property, value) {
  dbGetQuery(
    conn,
    paste(
      "SET",
      paste(property, value, sep = "=")
    )
  )
})

#' @rawNamespace
#' if (utils::packageVersion("dbplyr") >= "2") {
#'   importFrom(dbplyr, dbplyr_edition)
#'   S3method(dbplyr_edition, spark_connection)
#' }
dbplyr_edition.spark_connection <- function(con) {
  as.integer(spark_config_value(con$config, "sparklyr.dbplyr.edition", 2L))
}
