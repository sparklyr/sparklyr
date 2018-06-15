connection_progress <- function(sc, env)
{
  if (is.null(env$jobs))
    env$jobs <- list()

  if (exists(".rs.api.addJob")) {
    connection_progress_context(sc, function() {
      tracker <- tracker <- invoke(sc$spark_context, "statusTracker")
      active <- invoke(tracker, "getActiveJobIds")

      # add new jobs
      for (jobId in active) {
        jobId <- as.character(jobId)
        if (!jobId %in% names(env$jobs)) {
          if ("show" %in% names(formals(.rs.api.addJob)))
            env$jobs[[jobId]] <- .rs.api.addJob("Spark Job", progressUnits = 101L, show = FALSE)
          else
            env$jobs[[jobId]] <- .rs.api.addJob("Spark Job", progressUnits = 101L)

          .rs.api.setJobStatus(env$jobs[[jobId]], paste("Job", jobId))
        }
      }

      # remove or update jobs
      for (jobId in names(env$jobs)) {
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
