
# given an environment and a host, return the name of an open Spark connection
# object to the host, if any
find_object <- function(env, host) {
  objs <- ls(env)
  for (name in objs) {
    x <- base::get(name, envir = env)
    if (inherits(x, "spark_connection") &&
        identical(to_host(x), host) &&
        connection_is_open(x)) {
      return(name)
    }
  }
}

# connection-specific actions possible with Spark connections
spark_actions <- function(scon) {
  icons <- system.file(file.path("icons"), package = "sparklyr")
  list(
    SparkUI = list(
        icon = file.path(icons, "spark-ui.png"),
        callback = function() {
          utils::browseURL(spark_web(scon))
        }
    ),
    Log = list(
        icon = file.path(icons, "spark-log.png"),
        callback = function() {
          file.edit(spark_log_file(scon))
        }
    ),
    Help = list(
        icon = file.path(icons, "help.png"),
        callback = function() {
          utils::browseURL("http://spark.rstudio.com")
        }
    )
  )
}

on_connection_opened <- function(scon, env, connectCall) {

  # RStudio v1.1 generic connection interface --------------------------------
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
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
      disconnectCode = function() {
        name <- find_object(env, host)
        if (!is.null(name))
          paste0("spark_disconnect(", find_object(env, host), ")")
        else
          ""
      },

      # table enumeration code
      listTables = function() {
        connection_list_tables(scon)
      },

      # column enumeration code
      listColumns = function(table) {
        connection_list_columns(scon, table)
      },

      # table preview code
      previewTable = function(table, rowLimit) {
        connection_preview_table(scon, table, rowLimit)
      },

      # other actions that can be executed on this connection
      actions = spark_actions(scon)
    )
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
      listTablesCode =  "sparklyr:::connection_list_tables(%s)",

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
  viewer <- external_viewer()
  if (!is.null(viewer))
    viewer$connectionUpdated(type = "Spark", host = to_host(scon), hint = hint)
}

connection_list_tables <- function(sc) {
  if (!is.null(sc) && connection_is_open(sc))
    sort(dbListTables(sc))
  else
    character()
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
