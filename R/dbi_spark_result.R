# DBISparkResult results.


#' DBI Spark Result.
#'
#' @slot sql character.
#' @slot df data.frame.
#' @slot lastFetch numeric.
#'
#' @keywords internal
#'
#' @export
setClass("DBISparkResult",
         contains = "DBIResult",
         slots = list(
           sql = "character",
           sdf = "spark_jobj",
           conn = "spark_connection",
           lastFetch = "numeric"
         )
)

setMethod("dbGetStatement", "DBISparkResult", function(res, ...) {
  res@sql
})

setMethod("dbIsValid", "DBISparkResult", function(dbObj, ...) {
  TRUE
})

setMethod("dbGetRowCount", "DBISparkResult", function(res, ...) {
  invoke(res@sdf, "count")
})

setMethod("dbGetRowsAffected", "DBISparkResult", function(res, ...) {
  invoke(res@sdf, "count")
})

setMethod("dbColumnInfo", "DBISparkResult", function(res, ...) {
  ""
})

setMethod("dbSendQuery", c("spark_connection", "character"), function(conn, statement, params = NULL, ...) {
  sql <- as.character(statement)

  sdf <- invoke(hive_context(conn), "sql", sql)
  rs <- new("DBISparkResult",
            sql = sql,
            conn = conn,
            sdf = sdf)
  rs
})

setMethod("dbGetQuery", c("spark_connection", "character"), function(conn, statement, ...) {
  # TODO: Use default dbGetQuery method defined in DBIConnection
  rs <- dbSendQuery(conn, statement, ...)
  on.exit(dbClearResult(rs))

  df <- tryCatch(
    dbFetch(rs, n = -1, ...),
    error = function(e) {
      warning(conditionMessage(e), call. = conditionCall(e))
      NULL
    }
  )

  if (!dbHasCompleted(rs)) {
    warning("Pending rows", call. = FALSE)
  }

  df
})

setMethod("dbFetch", "DBISparkResult", function(res, n = -1, ..., row.names = NA) {
  start <- 1
  end <- n
  if (length(res@lastFetch) > 0) {
    start <- res@lastFetch + 1
    end <- res@lastFetch + end
  }

  res@lastFetch = end

  df <- df_from_sdf(res@conn, res@sdf, end)

  if (n > 0) {
    df <- df[start:end, ]
  }

  dfFetch <- as.data.frame(df, drop = FALSE, optional = TRUE)
  colnames(dfFetch) <- colnames(df)

  dfFetch
})

setMethod("dbBind", "DBISparkResult", function(res, params, ...) {
  TRUE
})

setMethod("dbHasCompleted", "DBISparkResult", function(res, ...) {
  TRUE
})

setMethod("dbClearResult", "DBISparkResult", function(res, ...) {
  TRUE
})
