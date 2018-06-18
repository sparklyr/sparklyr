connection_progress_update <- function(jobName, progressUnits)
{
  if ("show" %in% names(formals(.rs.api.addJob)))
    .rs.api.addJob(jobName, progressUnits = progressUnits, show = FALSE, autoRemove = FALSE)
  else
    .rs.api.addJob(jobName, progressUnits = progressUnits, autoRemove = FALSE)
}

connection_progress <- function(sc, terminated = FALSE)
{
  env <- sc$state$progress

  if (!exists(".rs.api.addJob"))
    return()

  if (is.null(env$jobs))
    env$jobs <- list()

  if (is.null(env$stages))
    env$stages <- list()

  if (!terminated || length(env$jobs) > 0) {
    connection_progress_context(sc, function() {
      tracker <- invoke(sc$spark_context, "statusTracker")
      active <- invoke(tracker, "getActiveJobIds")

      # add new jobs
      for (jobId in active) {
        jobId <- as.character(jobId)
        if (!jobId %in% names(env$jobs)) {
          jobName <- paste("Spark Job (", jobId, ")", sep = "")
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

          .rs.api.setJobStatus(env$jobs[[jobId]], jobStatus)
          stages <- invoke(jobInfo, "stageIds")

          # add new stages
          for (stageId in stages) {
            stageId <- as.character(stageId)
            if (!stageId %in% names(env$stages)) {
              stageName <- paste("Spark Stage (", stageId, ")", sep = "")
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

                .rs.api.setJobStatus(env$stages[[stageId]], stageStatusText)
                .rs.api.addJobProgress(env$stages[[stageId]], 1L)
              }
            } else {
              .rs.api.addJobProgress(env$stages[[stageId]], 100)
              env$stages[[stageId]] <- NULL
            }
          }
        }

        if (as.numeric(jobId) %in% active) {
          .rs.api.addJobProgress(env$jobs[[jobId]], 1L)
        } else {
          .rs.api.addJobProgress(env$jobs[[jobId]], 100)
          env$jobs[[jobId]] <- NULL
        }
      }
    })
  }

  if (terminated) {
    for (jobId in names(env$jobs))
      .rs.api.addJobProgress(env$jobs[[jobId]], 100L)
    for (stageId in names(env$stages))
      .rs.api.addJobProgress(env$stages[[stageId]], 100L)
  }
}

connection_progress_context <- function(sc, f)
{
  sc$state$use_monitoring <- TRUE
  on.exit(sc$state$use_monitoring <- FALSE)

  f()
}
