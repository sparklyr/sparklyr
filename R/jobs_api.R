# nocov start

rstudio_jobs_api_available <- function() {
  exists(".rs.api.addJob")
}

rstudio_jobs_api <- function() {
  list(
    add_job = get(".rs.api.addJob"),
    set_job_status = get(".rs.api.setJobStatus"),
    add_job_progress = get(".rs.api.addJobProgress"),
    remove_job = get(".rs.api.removeJob")
  )
}

rstudio_jobs_api_new <- function(jobName, progressUnits, jobActions) {
  api <- rstudio_jobs_api()
  if ("actions" %in% names(formals(api$add_job)) && !is.null(jobActions)) {
    api$add_job(jobName,
      progressUnits = progressUnits,
      show = FALSE,
      autoRemove = FALSE,
      actions = jobActions
    )
  } else if ("show" %in% names(formals(api$add_job))) {
    api$add_job(jobName,
      progressUnits = progressUnits,
      show = FALSE,
      autoRemove = FALSE
    )
  } else {
    api$add_job(jobName,
      progressUnits = progressUnits,
      autoRemove = FALSE
    )
  }
}

# nocov end
