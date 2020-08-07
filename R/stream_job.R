#' @importFrom rstudioapi sendToConsole
stream_register <- function(stream) {
  sc <- spark_connection(stream)

  if (is.null(sc$state$streams)) sc$state$streams <- new.env()
  sc$state$streams[[spark_jobj_id(stream)]] <- stream

  if (spark_config_logical(sc$config, "sparklyr.progress", TRUE) && rstudio_jobs_api_available()) {
    api <- rstudio_jobs_api()
    streamId <- stream_id(stream)

    jobActions <- list(
      info = function(id) {
        named_sc <- Filter(function(e) identical(get(e, envir = .GlobalEnv), sc), ls(envir = .GlobalEnv))[[1]]
        if (nchar(named_sc) == 0) {
          named_sc <- paste0(
            "spark_connection_find(",
            "method = \"", sc$method, "\", ",
            "master = \"", sc$master, "\", ",
            "app_name = \"", sc$app_name, "\"",
            ")"
          )
        }

        sendToConsole(
          paste0(
            "stream_find(", named_sc, ", \"", streamId, "\")",
            " %>% ",
            "stream_view()"
          )
        )
      },
      stop = function(id) {
        stream_stop(stream)
      }
    )

    stream$job <- rstudio_jobs_api_new(
      paste("Spark Stream", streamId),
      100L,
      jobActions
    )

    api$add_job_progress(stream$job, 10L)
  }

  stream
}

stream_unregister <- function(stream) {
  if (is.null(stream)) {
    return()
  }

  sc <- spark_connection(stream)

  if (!is.null(sc$state$streams)) {
    stream_id <- spark_jobj_id(stream)
    stream_ref <- sc$state$streams[[stream_id]]
    if (!is.null(stream_ref)) {
      if (!is.null(stream_ref$job)) rstudio_jobs_api()$remove_job(stream_ref$job)

      sc$state$streams[[stream_id]] <- NULL
    }
  }
}

stream_unregister_all <- function(sc) {
  for (stream_id in names(sc$state$streams)) {
    stream <- sc$state$streams[[stream_id]]
    stream_unregister(stream)
  }
}
