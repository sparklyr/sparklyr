# nocov start

#' Set of functions to provide integration with the RStudio IDE
#' @include browse_url.R
#' @details These function are meant for downstream packages, that provide additional
#' backends to `sparklyr`, to override the opening, closing, update, and preview
#' functionality. The arguments are driven by what the RStudio IDE API expects them
#' to be, so this is the reason why some use `type` to designated views or tables,
#' and others have one argument for `table`, and another for `view`.
#'
#' @param con Valid Spark connection
#' @param env R environment of the interactive R session
#' @param connect_call R code that can be used to re-connect to the Spark connection
#' @param hint Name of the Spark connection that the RStudio IDE can use as reference.
#' @param catalog Name of the top level of the requested table or view
#' @param schema Name of the second most top level of the requested level or view
#' @param name The new of the view or table being requested
#' @param type Type of the object being requested, 'view' or 'table'
#' @param table Name of the requested table
#' @param view Name of the requested view
#' @param rowLimit The number of rows to show in the 'Preview' pane of the RStudio
#' IDE
#' @export
spark_ide_connection_open <- function(con, env, connect_call) {
  UseMethod("spark_ide_connection_open")
}

#' @export
spark_ide_connection_open.default <- function(con, env, connect_call) {
  on_connection_opened(
    scon = con,
    env = env,
    connectCall = connect_call
  )
}

on_connection_opened <- function(scon, env, connectCall) {
  # RStudio v1.1 generic connection interface
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    tryCatch(
      {
        host <- to_host(scon)
        observer$connectionOpened(
          # connection type
          type = "Spark",

          # name displayed in connection pane
          displayName = to_host_display(scon),

          # host key
          host = host,

          # icon for connection
          icon = system.file(file.path("icons", "spark.png"), package = "sparklyr"),

          # connection code
          connectCode = connectCall,

          # disconnection code
          disconnect = function() {
            spark_disconnect(scon)
          },
          listObjectTypes = function() {
            return(
              list(catalog = list(
                contains =
                  list(schema = list(
                    contains =
                      list(
                        table = list(contains = "data"),
                        view = list(contains = "data")
                      )
                  ))
              ))
            )
          },

          # table enumeration code
          listObjects = function(...) {
            spark_ide_objects(scon, ...)
          },

          # column enumeration code
          listColumns = function(...) {
            spark_ide_columns(scon, ...)
          },

          # table preview code
          previewObject = function(rowLimit, ...) {
            spark_ide_preview(scon, rowLimit, ...)
          },

          # other actions that can be executed on this connection
          actions = spark_ide_connection_actions(scon),

          # raw connection object
          connectionObject = scon
        )
      },
      error = function(e) NULL
    )
  }

  # RStudio v1.0 Spark-style connection interface
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer)) {
    viewer$connectionOpened(
      # connection type
      type = "Spark",

      # host (unique identifier within type, used as default name)
      host = to_host(scon),

      # finder function
      finder = function(env, host) {
        # R CMD check doesn't like our using ::: to access to_host in the
        # finder function
        to_host <- get("to_host", asNamespace("sparklyr"))
        objs <- ls(env)
        for (name in objs) {
          x <- base::get(name, envir = env)
          if (inherits(x, "spark_connection") &&
            identical(to_host(x), host) &&
            sparklyr::connection_is_open(x)) {
            return(name)
          }
        }
        NULL
      },

      # connection code
      connectCode = connectCall,

      # disconnection code (object name will be determined via finder)
      disconnectCode = "spark_disconnect(%s)",

      # table enumeration code
      listTablesCode = "sparklyr:::connection_list_tables(%s, includeType = FALSE)",

      # column enumeration code
      listColumnsCode = "sparklyr:::connection_list_columns(%s, '%s')",

      # table preview code
      previewTableCode = "sparklyr:::connection_preview_table(%s, '%s', %s)"
    )
  }
}

# ---------------------------- Close Connection --------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_connection_closed <- function(con) {
  UseMethod("spark_ide_connection_closed")
}

#' @export
spark_ide_connection_closed.default <- function(con) {
  on_connection_closed(scon = con)
}

on_connection_closed <- function(scon) {
  viewer <- external_viewer()
  if (!is.null(viewer)) {
    viewer$connectionClosed(type = "Spark", host = to_host(scon))
  }
}

# ---------------------------- Update Connection -------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_connection_updated <- function(con, hint) {
  UseMethod("spark_ide_connection_updated")
}

#' @export
spark_ide_connection_updated.default <- function(con, hint) {
  on_connection_updated(scon = con, hint = hint)
}

on_connection_updated <- function(scon, hint) {
  # avoid updating temp tables that are filtered out
  if (grepl("^sparklyr_tmp_", hint)) {
    return()
  }

  viewer <- external_viewer()
  if (!is.null(viewer)) {
    viewer$connectionUpdated(type = "Spark", host = to_host(scon), hint = hint)
  }
}

# --------------------------- Action buttons -----------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_connection_actions <- function(con) {
  UseMethod("spark_ide_connection_actions")
}

#' @export
spark_ide_connection_actions.default <- function(con) {
  spark_ide_actions(scon = con)
}

spark_ide_actions <- function(scon) {
  icons <- system.file(file.path("icons"), package = "sparklyr")

  url <- spark_web(scon)

  if (as.character(url) == "") {
    actions <- list()
  } else {
    actions <- list(
      "Spark" = list(
        icon = file.path(icons, "spark-ui.png"),
        callback = function() {
          browse_url(url)
        }
      )
    )
  }

  if (spark_connection_is_yarn(scon)) {
    actions <- c(
      actions,
      list(
        "YARN" = list(
          icon = file.path(icons, "yarn-ui.png"),
          callback = function() {
            browse_url(spark_connection_yarn_ui(scon))
          }
        )
      )
    )
  }

  if (identical(tolower(scon$method), "livy")) {
    actions <- c(
      actions,
      list(
        "Livy" = list(
          icon = file.path(icons, "livy-ui.png"),
          callback = function() {
            utils::browseURL(file.path(scon$master, "ui"))
          }
        ),
        "Log" = list(
          icon = file.path(icons, "spark-log.png"),
          callback = function() {
            utils::browseURL(
              file.path(scon$master, "ui", "session", scon$sessionId, "log")
            )
          }
        )
      )
    )
  } else {
    if (length(as.character(spark_log(scon))) > 1) {
      actions <- c(
        actions,
        list(
          Log = list(
            icon = file.path(icons, "spark-log.png"),
            callback = function() {
              file.edit(spark_log_file(scon))
            }
          )
        )
      )
    }
  }

  if (exists(".rs.api.documentNew")) {
    documentNew <- get(".rs.api.documentNew")
    actions <- c(
      actions,
      list(
        SQL = list(
          icon = file.path(icons, "edit-sql.png"),
          callback = function() {
            varname <- "sc"
            if (!exists("sc", envir = .GlobalEnv) ||
              !identical(get("sc", envir = .GlobalEnv), scon)) {
              varname <- Filter(
                function(e) identical(get(e, envir = .GlobalEnv), scon),
                ls(envir = .GlobalEnv)
              )

              if (identical(length(varname), 0L)) {
                varname <- ""
              } else {
                varname <- varname[[1]]
              }
            }

            tables <- dbListTables(scon)

            contents <- paste(
              paste("-- !preview conn=", varname, sep = ""),
              "",
              if (length(tables) > 0) {
                paste("SELECT * FROM `", tables[[1]], "` LIMIT 1000", sep = "")
              } else {
                "SELECT 1"
              },
              "",
              sep = "\n"
            )

            documentNew("sql", contents, row = 2, column = 15, execute = FALSE)
          }
        )
      )
    )
  }

  actions <- c(
    actions,
    list(
      Help = list(
        icon = file.path(icons, "help.png"),
        callback = function() {
          utils::browseURL("https://spark.rstudio.com")
        }
      )
    )
  )

  actions
}

# ----------------------------- DB Objects -------------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_objects <- function(con, catalog, schema, name, type) {
  UseMethod("spark_ide_objects")
}

#' @export
spark_ide_objects.spark_connection <- function(con, catalog, schema, name, type) {
  connection_list_tables(con, includeType = TRUE)
}

connection_list_tables <- function(con, includeType = FALSE) {
  # extract a list of Spark tables
  tables <- if (!is.null(con) && connection_is_open(con)) {
    sort(dbListTables(con))
  } else {
    character()
  }

  # return the raw list of tables, or a data frame with object names and types
  if (includeType) {
    data.frame(
      name = tables,
      type = rep_len("table", length(tables)),
      stringsAsFactors = FALSE
    )
  } else {
    tables
  }
}

# ----------------------------- DB Columns -------------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_columns <- function(con,
                              table = NULL,
                              view = NULL,
                              catalog = NULL,
                              schema = NULL) {
  UseMethod("spark_ide_columns")
}

#' @export
spark_ide_columns.spark_connection <- function(con,
                                               table = NULL,
                                               view = NULL,
                                               catalog = NULL,
                                               schema = NULL) {
  connection_list_columns(con, table = table)
}

connection_list_columns <- function(con, table) {
  if (!is.null(con) && connection_is_open(con)) {
    sql <- paste("SELECT * FROM", table, "LIMIT 5")
    df <- dbGetQuery(con, sql)
    data.frame(
      name = names(df),
      type = as.character(lapply(names(df), function(f) {
        capture.output(str(df[[f]],
          give.length = FALSE,
          width = 30,
          strict.width = "cut"
        ))
      })),
      stringsAsFactors = FALSE
    )
  } else {
    NULL
  }
}

# ----------------------------- DB Preview -------------------------------------
#' @rdname spark_ide_connection_open
#' @export
spark_ide_preview <- function(
    con,
    rowLimit,
    table = NULL,
    view = NULL,
    catalog = NULL,
    schema = NULL) {
  UseMethod("spark_ide_preview")
}

#' @export
spark_ide_preview.spark_connection <- function(
    con,
    rowLimit,
    table = NULL,
    view = NULL,
    catalog = NULL,
    schema = NULL) {
  connection_preview_table(con, table, rowLimit)
}

connection_preview_table <- function(con, table, limit) {
  if (!is.null(con) && connection_is_open(con)) {
    sql <- paste("SELECT * FROM", table, "LIMIT", limit)
    dbGetQuery(con, sql)
  } else {
    NULL
  }
}

# --------------------------------- Utils --------------------------------------

# function to generate host display name
to_host_display <- function(sc) {
  gsub("local\\[(\\d+|\\*)\\]", "local", sc$master)
}

# function to convert master to host
to_host <- function(sc) {
  paste0(to_host_display(sc), " - ", sc$app_name)
}

# return the external connection viewer (or NULL if none active)
external_viewer <- function() {
  viewer <- getOption("connectionObserver")
  if (is.null(viewer)) {
    getOption("connectionViewer")
  } else {
    viewer
  }
}

# nocov end
