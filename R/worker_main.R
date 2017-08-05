spark_worker_main <- function(
  sessionId,
  backendPort = 8880,
  configRaw = worker_config_serialize(list())) {

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

    sc <- spark_worker_connect(sessionId, backendPort, config)
    worker_log("is connected")

    spark_worker_apply(sc)

  }, error = function(e) {
    stop("terminated unexpectedly: ", e$message)
  })

  worker_log("finished")
}

spark_worker_hooks <- function() {
  unlock <- get("unlockBinding")
  lock <- get("lockBinding")

  unlock("stop",  as.environment("package:base"))
  assign("stop", function(...) {
    worker_log_error(...)

    frame_names <- list()
    frame_start <- max(1, sys.nframe() - 5)
    for (i in frame_start:sys.nframe()) {
      current_call <- sys.call(i)
      frame_names[[1 + i - frame_start]] <- paste(i, ": ", paste(head(deparse(current_call), 5), collapse = "\n"), sep = "")
    }
    worker_log_error("collected callstack: \n", paste(rev(frame_names), collapse = "\n"))
    quit(status = -1)
  }, as.environment("package:base"))
  lock("stop",  as.environment("package:base"))
}
