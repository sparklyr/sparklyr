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
  # Retrieving an empty result
  tinyres <- dbSendQuery(res@conn, res@sql)
  tinyres@sdf <- tinyres@sdf %>% invoke("limit", as.integer(1))
  sdf_columns <- tinyres@sdf %>% invoke("dtypes")
  df <- dbFetch(tinyres, n=0)

  columns <- colnames(df)
  columns_types <- sapply(df, function (x) class(x)[1])
  columns_sql_types <- sapply(sdf_columns, function(x) { x %>% invoke("_2") })

  columns_info <- data.frame(
    name=columns,
    type=columns_types,
    sql.type=columns_sql_types
  )
  rownames(columns_info) <- NULL

  columns_info
})

setMethod(
  "dbSendQuery",
  "spark_connection",
  function(conn, statement, ..., params = list()) {
    sql <- as.character(DBI::sqlInterpolate(conn, statement, ...))

    if (spark_version(conn) < "3.4.0" && hasArg("params")) {
      stop("Native paramtereized queries require Spark 3.4.0 or newer")
    }

    if(length(params) == 0) {
      sdf <- invoke(hive_context(conn), "sql", sql)
    } else {
      sdf <- invoke(hive_context(conn), "sql", sql, as.environment(params))
    }

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
      end <- min(nrow(df), end)
      range <- start:end
    }
    df <- df[range, ]
  } else if (n == 0) {
    df <- df[0,]
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
