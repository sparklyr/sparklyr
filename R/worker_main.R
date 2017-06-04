spark_worker_main <- function(sessionId) {
  log_session(sessionId)
  log("is starting")

  tryCatch({

    sc <- spark_worker_connect(sessionId)
    log("is connected")

    spark_worker_apply(sc)

  }, error = function(e) {
    stop("terminated unexpectedly: ", e)
  })

  log("finished")
}
