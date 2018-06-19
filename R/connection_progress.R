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

connection_progress_update <- function(jobName, progressUnits)
{
  api <- connection_progress_api()
  if ("show" %in% names(formals(api$add_job)))
    api$add_job(jobName, progressUnits = progressUnits, show = FALSE, autoRemove = FALSE)
  else
    api$add_job(jobName, progressUnits = progressUnits, autoRemove = FALSE)
}

connection_progress <- function(sc, terminated = FALSE)
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
          env$jobs[[jobId]] <- connection_progress_update(jobName, 101L)
        }
      }

      # remove or update jobs
      for (jobId in names(env$jobs)) {
        jobInfoOption <- invoke(tracker, "getJobInfo", as.integer(jobId))
        if (invoke(jobInfoOption, "nonEmpty"))
        {
          jobInfo <- invoke(jobInfoOption, "get")
          jobStatus <- invoke(invoke(jobInfo, "status"), "toString")

          api$set_job_status(env$jobs[[jobId]], jobStatus)
          stages <- invoke(jobInfo, "stageIds")

          # add new stages
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
              env$stages[[stageId]] <- connection_progress_update(stageName, 101L)
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

                api$set_job_status(env$stages[[stageId]], stageStatusText)
                api$add_job_progress(env$stages[[stageId]], 1L)
              }
            } else {
              api$add_job_progress(env$stages[[stageId]], 100)
              env$stages[[stageId]] <- NULL
            }
          }
        }

        if (as.numeric(jobId) %in% active) {
          api$add_job_progress(env$jobs[[jobId]], 1L)
        } else {
          api$add_job_progress(env$jobs[[jobId]], 100)
          env$jobs[[jobId]] <- NULL
        }
      }
    })
  }

  if (terminated) {
    for (jobId in names(env$jobs))
      api$add_job_progress(env$jobs[[jobId]], 100L)
    for (stageId in names(env$stages))
      api$add_job_progress(env$stages[[stageId]], 100L)
  }
}

connection_progress_context <- function(sc, f)
{
  sc$state$use_monitoring <- TRUE
  on.exit(sc$state$use_monitoring <- FALSE)

  f()
}


connection_progress_terminated <- function(sc)
{
  connection_progress(sc, terminated = TRUE)
}
