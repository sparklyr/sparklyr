# nocov start

# connection-specific actions possible with Spark connections
spark_actions <- function(scon) {
  icons <- system.file(file.path("icons"), package = "sparklyr")

  actions <- list(
    "Spark" = list(
      icon = file.path(icons, "spark-ui.png"),
      callback = function() {
        utils::browseURL(spark_web(scon))
      }
    )
  )

  if (spark_connection_is_yarn(scon))
  {
    actions <- c(
      actions,
      list(
        "YARN" = list(
          icon = file.path(icons, "yarn-ui.png"),
          callback = function() {
            utils::browseURL(spark_connection_yarn_ui(scon))
          }
        )
      )
    )
  }

  if (identical(tolower(scon$method), "livy"))
  {
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
            utils::browseURL(file.path(scon$master, "ui", "session", scon$sessionId, "log"))
          }
        )
      )
    )
  }
  else
  {
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
                ls(envir = .GlobalEnv))

              if (identical(length(varname), 0L))
                varname <- ""
              else
                varname <- varname[[1]]
            }

            tables <- dbListTables(scon)

            contents <- paste(
              paste("-- !preview conn=", varname, sep = ""),
              "",
              if (length(tables) > 0)
                paste("SELECT * FROM `", tables[[1]], "` LIMIT 1000", sep = "")
              else
                "SELECT 1",
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
          utils::browseURL("http://spark.rstudio.com")
        }
      )
    )
  )

  actions
}

on_connection_opened <- function(scon, env, connectCall) {

  # RStudio v1.1 generic connection interface --------------------------------
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    tryCatch({
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

        listObjectTypes = function () {
          return(list(
            table = list(contains = "data")))
        },

        # table enumeration code
        listObjects = function(type = "table") {
          connection_list_tables(scon, includeType = TRUE)
        },

        # column enumeration code
        listColumns = function(table) {
          connection_list_columns(scon, table)
        },

        # table preview code
        previewObject = function(rowLimit, table) {
          connection_preview_table(scon, table, rowLimit)
        },

        # other actions that can be executed on this connection
        actions = spark_actions(scon),

        # raw connection object
        connectionObject = scon
      )
    }, error = function(e) NULL)
  }

  # RStudio v1.0 Spark-style connection interface ----------------------------
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
      listTablesCode =  "sparklyr:::connection_list_tables(%s, includeType = FALSE)",

      # column enumeration code
      listColumnsCode = "sparklyr:::connection_list_columns(%s, '%s')",

      # table preview code
      previewTableCode = "sparklyr:::connection_preview_table(%s, '%s', %s)"
    )
  }
}

# return the external connection viewer (or NULL if none active)
external_viewer <- function() {
  viewer <- getOption("connectionObserver")
  if (is.null(viewer))
    getOption("connectionViewer")
  else
    viewer
}

on_connection_closed <- function(scon) {
  viewer <- external_viewer()
  if (!is.null(viewer))
    viewer$connectionClosed(type = "Spark", host = to_host(scon))
}

on_connection_updated <- function(scon, hint) {
  # avoid updating temp tables that are filtered out
  if (grepl("^sparklyr_tmp_", hint)) return();

  viewer <- external_viewer()
  if (!is.null(viewer))
    viewer$connectionUpdated(type = "Spark", host = to_host(scon), hint = hint)
}

connection_list_tables <- function(sc, includeType = FALSE) {
  # extract a list of Spark tables
  tables <- if (!is.null(sc) && connection_is_open(sc))
    sort(dbListTables(sc))
  else
    character()

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

connection_list_columns <- function(sc, table) {
  if (!is.null(sc) && connection_is_open(sc)) {
    sql <- paste("SELECT * FROM", table, "LIMIT 5")
    df <- dbGetQuery(sc, sql)
    data.frame(
      name = names(df),
      type = as.character(lapply(names(df), function(f) {
        capture.output(str(df[[f]],
                           give.length = FALSE,
                           width = 30,
                           strict.width = "cut"))
      })),
      stringsAsFactors = FALSE
    )
  } else {
    NULL
  }
}

connection_preview_table <- function(sc, table, limit) {
  if (!is.null(sc) && connection_is_open(sc)) {
    sql <- paste("SELECT * FROM", table, "LIMIT", limit)
    dbGetQuery(sc, sql)
  } else {
    NULL
  }
}

# function to generate host display name
to_host_display <- function(sc) {
  gsub("local\\[(\\d+|\\*)\\]", "local", sc$master)
}

# function to convert master to host
to_host <- function(sc) {
  paste0(to_host_display(sc), " - ", sc$app_name)
}

# nocov end
