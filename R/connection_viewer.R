

on_connection_opened <- function(scon, connectCall) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer)) {
    viewer$connectionOpened(
      # connection type
      type = "Spark",

      # host (unique identifier within type, used as default name)
      host = scon$master,

      # finder function
      function(env, host) {
        objs <- ls(env)
        for (name in objs) {
          x <- get(name, envir = env)
          if (inherits(x, "spark_connection") &&
              identical(x$master, host)) {
            return(x)
          }
        }
        NULL
      },

      # connection code
      paste("library(rspark)\nsc <-", connectCall),

      # disconnection code (object name will be determined via finder)
      "library(rspark)\nspark_disconnect(%s)")
  }
}

on_connection_closed <- function(scon) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionClosed(type = "Spark", host = scon$master)
}

on_connection_updated <- function(scon) {
  viewer <- getOption("connectionViewer")
  if (!is.null(viewer))
    viewer$connectionUpdated(type = "Spark", host = scon$master)
}
