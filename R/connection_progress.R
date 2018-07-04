# nocov start

connection_progress_api_available <- function() {
  exists(".rs.api.addJob")
}

connection_progress_api <- function() {
  list(
    add_job = get(".rs.api.addJob"),
    set_job_status = get(".rs.api.setJobStatus"),
    add_job_progress = get(".rs.api.addJobProgress")
  )
}

connection_progress_update <- function(jobName, progressUnits, url)
{
  api <- connection_progress_api()
  if ("actions" %in% names(formals(api$add_job)) && nchar(url) > 0) {
    jobActions <- NULL
    if (nchar(url) > 0) {
      jobActions <- list(
        info = function(id) {
          browseURL(url)
        }
      )
    }

    api$add_job(jobName,
                progressUnits = progressUnits,
                show = FALSE,
                autoRemove = FALSE,
                actions = jobActions)
  } else if ("show" %in% names(formals(api$add_job))) {
    api$add_job(jobName,
                progressUnits = progressUnits,
                show = FALSE,
                autoRemove = FALSE)
  } else {
    api$add_job(jobName,
                progressUnits = progressUnits,
                autoRemove = FALSE)
  }
}

connection_progress_base <- function(sc, terminated = FALSE)
{
  env <- sc$state$progress

  if (!connection_progress_api_available())
    return()

  api <- connection_progress_api()

  if (is.null(env$jobs))
    env$jobs <- list()

  if (is.null(env$stages))
    env$stages <- list()

  if ((!terminated || length(env$jobs) > 0) &&
      !is.null(sc$spark_context)) {
    connection_progress_context(sc, function() {
      if (is.null(env$web_url)) {
        env$web_url <- tryCatch({
          spark_web(sc)
        }, error = function(e) {
          ""
        })
      }

      tracker <- invoke(sc$spark_context, "statusTracker")
      active <- invoke(tracker, "getActiveJobIds")

      # add new jobs
      for (jobId in active) {
        jobId <- as.character(jobId)
        if (!jobId %in% names(env$jobs)) {
          jobIdText <- ""
          jobInfoOption <- invoke(tracker, "getJobInfo", as.integer(jobId))
          if (invoke(jobInfoOption, "nonEmpty"))
          {
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
        if (invoke(jobInfoOption, "nonEmpty"))
        {
          jobInfo <- invoke(jobInfoOption, "get")
          jobStatus <- invoke(invoke(jobInfo, "status"), "toString")

          api$set_job_status(env$jobs[[jobId]]$ref, jobStatus)
          stages <- invoke(jobInfo, "stageIds")

          # add new stages
          if (spark_config_logical(sc$config, "sparklyr.progress.stages", FALSE)) {
            for (stageId in stages) {
              stageId <- as.character(stageId)
              if (!stageId %in% names(env$stages)) {
                stageIdText <- ""
                stageInfoOption <- invoke(tracker, "getStageInfo", as.integer(stageId))
                if (invoke(stageInfoOption, "nonEmpty"))
                {
                  stageInfo <- invoke(stageInfoOption, "get")
                  stageSparkId <- invoke(stageInfo, "stageId")
                  stageIdText <- paste0("(", stageSparkId, ")", sep = "")
                }

                stageName <- paste("Spark Stage", stageIdText)
                stageUrl <- file.path(env$web_url, "stages", "stage", paste0("?id=", jobSparkId, "&attempt=0"))
                stageUrlParam <- if (nchar(env$web_url) > 0) stageUrl else ""
                env$stages[[stageId]] <- list(
                  ref = connection_progress_update(stageName, 101L, stageUrlParam),
                  units = 1
                )
              }
            }


            # remove or update stages
            for (stageId in names(env$stages)) {
              if (as.numeric(stageId) %in% stages) {
                stageInfoOption <- invoke(tracker, "getStageInfo", as.integer(stageId))
                if (invoke(stageInfoOption, "nonEmpty"))
                {
                  stageInfo <- invoke(stageInfoOption, "get")

                  stageTasks <- invoke(stageInfo, "numTasks")
                  stageCompleted <- invoke(stageInfo, "numCompletedTasks")
                  stageStatusText <- paste0(stageCompleted, "/", stageTasks, " completed")

                  api$set_job_status(env$stages[[stageId]]$ref, stageStatusText)
                  if (env$stages[[stageId]]$units < 100) {
                    api$add_job_progress(env$stages[[stageId]]$ref, 1L)
                    env$stages[[stageId]]$units <- env$stages[[stageId]]$units + 1
                  }
                }
              } else {
                api$add_job_progress(env$stages[[stageId]]$ref, 100)
                env$stages[[stageId]] <- NULL
              }
            }
          }
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
    for (jobId in names(env$jobs))
      api$add_job_progress(env$jobs[[jobId]]$ref, 100L)
    for (stageId in names(env$stages))
      api$add_job_progress(env$stages[[stageId]]$ref, 100L)
  }
}

connection_progress_context <- function(sc, f)
{
  sc$state$use_monitoring <- TRUE
  on.exit(sc$state$use_monitoring <- FALSE)

  sc$config$sparklyr.backend.timeout <- 1

  f()
}

connection_progress <- function(sc, terminated = FALSE)
{
  if (!spark_config_logical(sc$config, "sparklyr.progress", TRUE)) return()

  tryCatch({
    connection_progress_base(sc, terminated)
  }, error = function(e) {
    # ignore all connection progress errors
  })
}

connection_progress_terminated <- function(sc)
{
  if (!spark_config_logical(sc$config, "sparklyr.progress", TRUE)) return()

  connection_progress(sc, terminated = TRUE)
}

# nocov end
