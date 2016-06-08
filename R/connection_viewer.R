

on_connection_opened <- function(scon, connectCall, db) {

  viewer <- getOption("connectionViewer")
  if (!is.null(viewer)) {

    # if this is a database connection then provide enumeration/preview functions
    if (db) {
      listTablesCode <- "rspark:::connection_list_tables(%s)"
      listColumnsCode <- "rspark:::connection_list_columns(%s, table)"
      previewTableCode <- "rspark:::connection_preview_table(%s, table)"
    } else {
      listTablesCode <- NULL
      listColumnsCode <- NULL
      previewTableCode <- NULL
    }

    viewer$connectionOpened(
      # connection type
      type = "Spark",

      # host (unique identifier within type, used as default name)
      host = scon$master,

      # finder function
      finder = function(env, host) {
        objs <- ls(env)
        for (name in objs) {
          x <- get(name, envir = env)
          if (inherits(x, "spark_connection") &&
              identical(x$master, host)) {
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
      listTablesCode = listTablesCode,

      # column enumeration code
      listColumnsCode = listColumnsCode,

      # table preview code
      previewTableCode = previewTableCode
    )
  }
}

on_connection_closed <- function(scon) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionClosed(type = "Spark", host = scon$master)
}

on_connection_updated <- function(scon, hint) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionUpdated(type = "Spark", host = scon$master, hint = hint)
}

connection_list_tables <- function(sc) {
  c("foo", "bar", "wins")
}

connection_list_columns <- function(sc, table) {
  c("a", "b", "c")
}

connection_preview_table <- function(sc, table) {
  NULL
}





