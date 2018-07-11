#' @importFrom rstudioapi sendToConsole
stream_register_job <- function(stream)
{
  if (rstudio_jobs_api_available()) {
    sc <- spark_connection(stream)
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
