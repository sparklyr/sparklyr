# nocov start

#' @include browse_url.R
NULL

connection_progress_update <- function(jobName, progressUnits, url) {
  jobActions <- NULL
  if (nchar(url) > 0) {
    jobActions <- list(
      info = function(id) {
        browse_url(url)
      }
    )
  }

  rstudio_jobs_api_new(
    jobName,
    progressUnits,
    jobActions
  )
}

connection_progress_base <- function(sc, terminated = FALSE) {
  env <- sc$state$progress
  api <- rstudio_jobs_api()

  if (is.null(env$jobs)) {
    env$jobs <- list()
  }

  if ((!terminated || length(env$jobs) > 0) &&
    !is.null(spark_context(sc))) {
    connection_progress_context(sc, function() {
      if (is.null(env$web_url)) {
        env$web_url <- tryCatch(
          {
            spark_web(sc)
          },
          error = function(e) {
            ""
          }
        )
      }

      tracker <- invoke(spark_context(sc), "statusTracker")
      active <- invoke(tracker, "getActiveJobIds")

      # add new jobs
      for (jobId in active) {
        jobId <- as.character(jobId)
        if (!jobId %in% names(env$jobs)) {
          jobIdText <- ""
          jobInfoOption <- invoke(tracker, "getJobInfo", as.integer(jobId))
          if (invoke(jobInfoOption, "nonEmpty")) {
            jobInfo <- invoke(jobInfoOption, "get")
            jobSparkId <- invoke(jobInfo, "jobId")
            jobIdText <- paste("(", jobSparkId, ")", sep = "")
          }

          jobName <- paste("Spark Job", jobIdText)
          jobUrl <- file.path(env$web_url, "jobs", "job", paste0("?id=", jobSparkId))
          jobUrlParam <- if (nchar(jobUrl) > 0) jobUrl else ""
          env$jobs[[jobId]] <- list(
            ref = connection_progress_update(jobName, 101L, jobUrlParam),
            units = 1
          )
        }
      }

      # remove or update jobs
      for (jobId in names(env$jobs)) {
        jobInfoOption <- invoke(tracker, "getJobInfo", as.integer(jobId))
        if (invoke(jobInfoOption, "nonEmpty")) {
          jobInfo <- invoke(jobInfoOption, "get")
          jobStatus <- invoke(invoke(jobInfo, "status"), "toString")

          api$set_job_status(env$jobs[[jobId]]$ref, jobStatus)
        }

        if (as.numeric(jobId) %in% active) {
          if (env$jobs[[jobId]]$units < 100) {
            api$add_job_progress(env$jobs[[jobId]]$ref, 1L)
            env$jobs[[jobId]]$units <- env$jobs[[jobId]]$units + 1
          }
        } else {
          api$add_job_progress(env$jobs[[jobId]]$ref, 100)
          env$jobs[[jobId]] <- NULL
        }
      }
    })
  }

  if (terminated) {
    for (jobId in names(env$jobs)) {
      api$add_job_progress(env$jobs[[jobId]]$ref, 100L)
    }
  }
}

connection_progress_context <- function(sc, f) {
  sc$state$use_monitoring <- TRUE
  on.exit(sc$state$use_monitoring <- FALSE)

  sc$config$sparklyr.backend.timeout <- 1

  f()
}

connection_progress <- function(sc, terminated = FALSE) {
  if (!spark_config_logical(sc$config, "sparklyr.progress", TRUE) ||
    !rstudio_jobs_api_available() ||
    identical(sc$state$use_monitoring, TRUE)) {
    return()
  }

  tryCatch(
    {
      connection_progress_base(sc, terminated)
    },
    error = function(e) {
      # ignore all connection progress errors
      if (spark_config_value(sc$config, "sparklyr.verbose", FALSE)) {
        warning("Error while checking job progress: ", e$message)
      }
    }
  )
}

connection_progress_terminated <- function(sc) {
  connection_progress(sc, terminated = TRUE)
}

# nocov end
