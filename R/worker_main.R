spark_worker_main <- function(sessionId) {
  log_session(sessionId)
  worker_log("is starting")

  tryCatch({

    sc <- spark_worker_connect(sessionId)
    worker_log("is connected")

    spark_worker_apply(sc)

  }, error = function(e) {
    stop("terminated unexpectedly: ", e)
  })

  worker_log("finished")
}
