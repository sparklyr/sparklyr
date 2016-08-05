

on_connection_opened <- function(scon, connectCall) {

  viewer <- getOption("connectionViewer")
  if (!is.null(viewer)) {

    viewer$connectionOpened(
      # connection type
      type = "Spark",

      # host (unique identifier within type, used as default name)
      host = to_host(scon),

      # finder function
      finder = function(env, host) {
        # we duplicate the to_host function here b/c R CMD check
        # doesn't like our using ::: to access to_host in the
        # finder function
        to_host <- function(sc) {
          paste0(gsub("local\\[(\\d+|\\*)\\]", "local", sc$master),
                 " - ",
                 sc$app_name)
        }
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

on_connection_closed <- function(scon) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionClosed(type = "Spark", host = to_host(scon))
}

on_connection_updated <- function(scon, hint) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionUpdated(type = "Spark", host = to_host(scon), hint = hint)
}

connection_list_tables <- function(sc) {
  dbi <- sc
  if (!is.null(dbi))
    sort(dbListTables(dbi))
  else
    character()
}

connection_list_columns <- function(sc, table) {
  dbi <- sc
  if (!is.null(dbi)) {
    sql <- paste("SELECT * FROM", table, "LIMIT 5")
    df <- dbGetQuery(dbi, sql)
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
  dbi <- sc
  if (!is.null(dbi)) {
    sql <- paste("SELECT * FROM", table, "LIMIT", limit)
    dbGetQuery(dbi, sql)
  } else {
    NULL
  }
}
# function to convert master to host
to_host <- function(sc) {
  paste0(gsub("local\\[(\\d+|\\*)\\]", "local", sc$master),
         " - ",
         sc$app_name)
}



