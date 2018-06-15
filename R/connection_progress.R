connection_progress_update <- function(jobName, progressUnits)
{
  if ("show" %in% names(formals(.rs.api.addJob)))
    .rs.api.addJob(jobName, progressUnits = progressUnits, show = FALSE)
  else
    .rs.api.addJob(jobName, progressUnits = progressUnits)
}

connection_progress <- function(sc, env)
{
  if (is.null(env$jobs))
    env$jobs <- list()

  if (is.null(env$stages))
    env$stages <- list()

  if (exists(".rs.api.addJob")) {
    connection_progress_context(sc, function() {
      tracker <- tracker <- invoke(sc$spark_context, "statusTracker")
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
          .rs.api.setJobStatus(env$jobs[[jobId]], invoke(jobInfo, "status"))
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

                stageStatus <- invoke(stageInfo, "name")
                stageTasks <- invoke(stageInfo, "numTasks")
                stageCompleted <- invoke(stageInfo, "numCompletedTasks")
                stageStatusText <- paste0(stageStatus, " (", stageCompleted, "/", stageTasks, ")")

                .rs.api.setJobStatus(env$jobs[[jobId]], stageStatusText)
                .rs.api.addJobProgress(env$stages[[stageId]], 1L)
              }
            } else {
              .rs.api.addJobProgress(env$stages[[stageId]], 100)
              env$jobs[[jobId]] <- NULL
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
}

connection_progress_context <- function(sc, f)
{
  sc$state$use_monitoring <- TRUE
  on.exit(sc$state$use_monitoring <- FALSE)

  f()
}
