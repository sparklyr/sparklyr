connection_progress <- function(sc, env)
{
  if (is.null(env$jobs))
    env$jobs <- list()

  if (exists(".rs.api.addJob")) {
    connection_progress_context(sc, {
      tracker <- invoke(spark_context(sc), "statusTracker")
      active <- invoke(tracker, "getActiveJobIds")

      # add new jobs
      for (jobId in active) {
        if (!jobId %in% names(env$jobs)) {
          if ("show" %in% names(formals(.rs.api.addJob)))
            env$jobs[[job]] <- .rs.api.addJob("Spark Job", progressUnits = 101L, show = FALSE)
          else
            env$jobs[[job]] <- .rs.api.addJob("Spark Job", progressUnits = 101L)

          .rs.api.setJobStatus(paste("Job", jobId))
        }
      }

      # remove or update jobs
      for (job in names(env$jobs)) {
        jobId <- as.numeric(job)
        if (jobId %in% active) {
          .rs.api.addJobProgress(env$progressRef, 1L)
        } else {
          .rs.api.addJobProgress(env$jobs[[job]], 100)
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
