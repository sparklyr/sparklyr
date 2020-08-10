# DBISparkResult results.


#' DBI Spark Result.
#'
#' @slot sql character.
#' @slot sdf spark_jobj.
#' @slot conn spark_connection.
#' @slot state environment.
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
    state = "environment"
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

setMethod(
  "dbSendQuery",
  "spark_connection",
  function(conn, statement, ...) {
    sql <- as.character(DBI::sqlInterpolate(conn, statement, ...))

    sdf <- invoke(hive_context(conn), "sql", sql)
    rs <- new("DBISparkResult",
      sql = sql,
      conn = conn,
      sdf = sdf,
      state = new.env()
    )
    rs
  }
)

setMethod(
  "dbGetQuery",
  # c("spark_connection", "character"),
  "spark_connection",
  function(conn, statement, ...) {
    rs <- dbSendQuery(conn, statement, ...)
    on.exit(dbClearResult(rs))

    df <- tryCatch(
      dbFetch(rs, n = -1, ...),
      error = function(e) {
        stop("Failed to fetch data: ", e$message)
      }
    )

    if (!dbHasCompleted(rs)) {
      warning("Pending rows", call. = FALSE)
    }

    if (ncol(df) > 0) {
      df
    } else {
      invisible(df)
    }
  }
)

setMethod("dbFetch", "DBISparkResult", function(res, n = -1, ..., row.names = NA) {
  start <- 1
  end <- n
  lastFetch <- res@state$lastFetch

  if (length(lastFetch) > 0) {
    start <- lastFetch + 1
    end <- lastFetch + end
  }

  res@state$lastFetch <- end
  df <- df_from_sdf(res@conn, res@sdf, end)

  if (n > 0) {
    range <- 0
    if (nrow(df) > 0) {
      end <- min(nrow(df), nrow(df))
      range <- start:end
    }
    df <- df[range, ]
  }

  if (nrow(df) == 0) {
    df <- df[]
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

setMethod("dbSendStatement", "spark_connection", function(conn, statement, ...) {
  dbSendQuery(conn, statement, ...)
})

setMethod("dbExecute", "spark_connection", function(conn, statement, ...) {
  rs <- dbSendStatement(conn, statement, ...)
  on.exit(dbClearResult(rs))
  dbGetRowsAffected(rs)
})
