spark_worker_main <- function(sessionId) {
  spark_worker_hooks()

  worker_log_session(sessionId)
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

spark_worker_hooks <- function() {
  unlockBinding("stop",  as.environment("package:base"))
  assign("stop", function(...) {
    worker_log_error(...)
    quit(status = -1)
  }, as.environment("package:base"))
  lockBinding("stop",  as.environment("package:base"))
}
