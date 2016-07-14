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
           df = "data.frame",
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
  nrow(res@df)
})

setMethod("dbGetRowsAffected", "DBISparkResult", function(res, ...) {
  nrow(res@df)
})

setMethod("dbColumnInfo", "DBISparkResult", function(res, ...) {
  ""
})

setMethod("dbSendQuery", c("spark_connection", "character"), function(conn, statement, params = NULL, ...) {
  df <- sdf_from_sql(conn, statement)

  rs <- new("DBISparkResult",
            df = df,
            sql = statement)
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
  if (n == -1 || NROW(res@df) < n)
    return(res@df)

  start <- 1
  end <- n
  if (length(res@lastFetch) > 0) {
    start <- res@lastFetch + 1
    end <- res@lastFetch + end
  }

  res@lastFetch = end

  dfFetch <- as.data.frame(res@df[start:end, ], drop = FALSE, optional = TRUE)
  colnames(dfFetch) <- colnames(res@df)

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
