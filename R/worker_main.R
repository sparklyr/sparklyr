spark_worker_main <- function(sessionId) {
  log("sparklyr worker starting")

  tryCatch({

    sc <- spark_worker_connect(sessionId)
    log("sparklyr worker connected")

    spark_worker_apply(sc)

  }, error = function(e) {
      stop("Worker terminated unexpectedly: " + e)
  })

  log("sparklyr worker finished")
}
