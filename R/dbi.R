# ---- DBI connection methods ----

setMethod("dbGetInfo", "spark_connection", function(dbObj, ...) {
  dbObj
})

setMethod("show", "spark_connection", function(object) {
  info <- dbGetInfo(object)

  cat("<spark_connection> ", info$master, "\n", sep = "")
})

setMethod("dbDisconnect", "spark_connection", function(conn) {
  spark_disconnect(conn)
})

setMethod("dbIsValid", "spark_connection", function(dbObj) {
  connection_is_open(dbObj)
})

# Determine database type for R vector.
setMethod("dbDataType", "spark_connection", function(dbObj, obj) {
  get_data_type(obj)
})

setMethod("sqlParseVariables", "spark_connection", function(conn, sql, ...) {
  method <- getMethod("sqlParseVariables", "DBIConnection")
  method(conn, sql, ...)
})

setMethod(
  "sqlInterpolate",
  "spark_connection",
  function(conn, sql, ..., .dots = list()) {
    method <- getMethod("sqlInterpolate", "DBIConnection")
    method(conn, sql, ..., .dots = .dots)
  }
)

setMethod("dbQuoteLiteral", "spark_connection", function(conn, x, ...) {
  method <- getMethod("dbQuoteLiteral", "DBIConnection")
  method(conn, x, ...)
})

get_data_type <- function(obj) {
  if (is.factor(obj)) {
    return("TEXT")
  }

  switch(
    typeof(obj),
    integer = "INTEGER",
    double = "REAL",
    character = "STRING",
    logical = "INTEGER",
    list = "BLOB",
    stop("Unsupported type", call. = FALSE)
  )
}

dbi_ensure_no_backtick <- function(x) {
  if (regexpr("`", x)[[1]] >= 0) {
    stop("Can't escape back tick from string")
  }
}

setMethod(
  "dbQuoteIdentifier",
  c("spark_connection", "character"),
  function(conn, x, ...) {
    if (is(x, "SQL")) {
      return(x)
    }
    split_x <- unlist(strsplit(x, "\\."))
    possible_schema <- FALSE
    if (length(split_x) > 1) {
      if (!any(split_x == "")) possible_schema <- TRUE
    }
    if (
      length(x) == 0L ||
        (inherits(x, "ident") && possible_schema) ||
        (regexpr("`", x)[[1]] >= 0 && possible_schema)
    ) {
      out <- x
    } else {
      dbi_ensure_no_backtick(x)
      y <- paste("`", x, "`", sep = "")
      out <- SQL(y)
    }
    out
  }
)

setMethod(
  "dbQuoteString",
  c("spark_connection", "character"),
  function(conn, x, ...) {
    SQL(paste('"', gsub('"', '\\\\"', x), '"', sep = ""))
  }
)

# Sets a property for the connection
setGeneric("dbSetProperty", function(conn, property, value) {
  standardGeneric("dbSetProperty")
})
# Sets a property for the connection
setMethod(
  "dbSetProperty",
  c("spark_connection", "character", "character"),
  function(conn, property, value) {
    dbGetQuery(
      conn,
      paste(
        "SET",
        paste(property, value, sep = "=")
      )
    )
  }
)

# ---- DBI Transactions ----

#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.spark_connection <- function(con) {
  as.integer(spark_config_value(con$config, "sparklyr.dbplyr.edition", 2L))
}


setMethod("dbBegin", "spark_connection", function(conn) {
  TRUE
})

setMethod("dbCommit", "spark_connection", function(conn) {
  TRUE
})

setMethod("dbRollback", "spark_connection", function(conn) {
  TRUE
})

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
setClass(
  "DBISparkResult",
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
  df <- dbFetch(tinyres, n = 0)

  columns <- colnames(df)
  columns_types <- sapply(df, function(x) class(x)[1])
  columns_sql_types <- sapply(sdf_columns, function(x) {
    x %>% invoke("_2")
  })

  columns_info <- data.frame(
    name = columns,
    type = columns_types,
    sql.type = columns_sql_types
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

    if (length(params) == 0) {
      sdf <- invoke(hive_context(conn), "sql", sql)
    } else {
      sdf <- invoke(hive_context(conn), "sql", sql, as.environment(params))
    }

    rs <- new(
      "DBISparkResult",
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

setMethod(
  "dbFetch",
  "DBISparkResult",
  function(res, n = -1, ..., row.names = NA) {
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
      df <- df[0, ]
    }

    if (nrow(df) == 0) {
      df <- df[]
    }

    dfFetch <- as.data.frame(df, drop = FALSE, optional = TRUE)
    colnames(dfFetch) <- colnames(df)

    dfFetch
  }
)

setMethod("dbBind", "DBISparkResult", function(res, params, ...) {
  TRUE
})

setMethod("dbHasCompleted", "DBISparkResult", function(res, ...) {
  TRUE
})

setMethod("dbClearResult", "DBISparkResult", function(res, ...) {
  TRUE
})

setMethod(
  "dbSendStatement",
  "spark_connection",
  function(conn, statement, ...) {
    dbSendQuery(conn, statement, ...)
  }
)

setMethod("dbExecute", "spark_connection", function(conn, statement, ...) {
  rs <- dbSendStatement(conn, statement, ...)
  on.exit(dbClearResult(rs))
  dbGetRowsAffected(rs)
})

setMethod(
  "dbWriteTable",
  "spark_connection",
  function(
    conn,
    name,
    value,
    temporary = getOption("sparklyr.dbwritetable.temp", FALSE),
    overwrite = FALSE,
    append = FALSE,
    repartition = 0,
    serializer = NULL
  ) {
    found <- dbExistsTable(conn, name) && !overwrite
    if (found) {
      stop("Table ", name, " already exists")
    }

    if (append && overwrite) {
      stop("append and overwrite parameters cannot both be setted to TRUE.")
    }

    temp_name <- if (identical(temporary, FALSE)) {
      random_string("sparklyr_tmp_")
    } else {
      name
    }

    if (identical(append, TRUE)) {
      mode <- "append"
    } else if (identical(overwrite, TRUE)) {
      mode <- "overwrite"
    } else {
      mode <- NULL
    }

    spark_data_copy(
      conn,
      value,
      temp_name,
      repartition,
      serializer = serializer
    )

    if (identical(temporary, FALSE)) {
      spark_write_table(
        tbl(conn, temp_name),
        name,
        mode
      )
    }

    invisible(TRUE)
  }
)

setMethod(
  "dbReadTable",
  c("spark_connection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)


setMethod("dbListTables", "spark_connection", function(conn, database = NULL) {
  query <- "SHOW TABLES"
  if (!is.null(database)) {
    query <- paste(query, "FROM", database, sep = " ")
  }
  df <- df_from_sql(conn, query)

  if (nrow(df) <= 0) {
    character(0)
  } else {
    tableNames <- df$tableName
    filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
    sort(filtered)
  }
})


setMethod(
  "dbExistsTable",
  c("spark_connection", "character"),
  function(conn, name) {
    tolower(name) %in% tolower(dbListTables(conn))
  }
)


setMethod(
  "dbRemoveTable",
  c("spark_connection", "character"),
  function(conn, name, ..., fail_if_missing = TRUE) {
    dbi_ensure_no_backtick(name)

    if_exists <- if (fail_if_missing) "" else "IF EXISTS "
    dbSendQuery(conn, paste0("DROP TABLE ", if_exists, "`", name, "`"))
    invisible(TRUE)
  }
)
