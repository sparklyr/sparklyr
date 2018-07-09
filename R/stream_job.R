stream_register_job <- function(stream)
{
  if (rstudio_jobs_api_available()) {
    api <- rstudio_jobs_api()
    name <- stream_name(stream)

    jobActions <- list(
      info = function(id) {
        stream_view(stream)
      },
      stop = function(id) {
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
