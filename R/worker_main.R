spark_worker_main <- function(sessionId, configRaw) {
  spark_worker_hooks()

  config <- worker_config_deserialize(configRaw)

  if (config$debug) {
    worker_log("exiting to wait for debugging session to attach")

    # sleep for 1 day to allow long debugging sessions
    Sys.sleep(60*60*24)
    return()
  }

  worker_log_session(sessionId)
  worker_log("is starting")

  tryCatch({

    sc <- spark_worker_connect(sessionId, config)
    worker_log("is connected")

    spark_worker_apply(sc)

  }, error = function(e) {
    stop("terminated unexpectedly: ", e)
  })

  worker_log("finished")
}

spark_worker_hooks <- function() {
  unlock <- get("unlockBinding")
  lock <- get("lockBinding")

  unlock("stop",  as.environment("package:base"))
  assign("stop", function(...) {
    worker_log_error(...)
    quit(status = -1)
  }, as.environment("package:base"))
  lock("stop",  as.environment("package:base"))
}
