#' @importFrom rstudioapi sendToConsole
stream_register_job <- function(stream)
{
  if (rstudio_jobs_api_available()) {
    api <- rstudio_jobs_api()
    id <- stream_id(stream)

    jobActions <- list(
      info = function(id) {
        sendToConsole(
          paste0("stream_view(\"", id, "\")")
        )
      },
      stop = function(id) {
        api$add_job_progress(job, 100L)
        stream_stop(stream)
      }
    )

    job <- rstudio_jobs_api_new(
      stream_name(stream),
      100L,
      jobActions
    )

    api$add_job_progress(job, 10L)
  }

  stream
}
