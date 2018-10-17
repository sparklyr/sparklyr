# nocov start

.worker_globals <- new.env(parent = emptyenv())

spark_worker_main <- function(
  sessionId,
  backendPort = 8880,
  configRaw = NULL) {

  spark_worker_hooks()

  tryCatch({
    worker_log_session(sessionId)

    if (is.null(configRaw)) configRaw <- worker_config_serialize(list())

    config <- worker_config_deserialize(configRaw)

    if (identical(config$profile, TRUE)) {
      profile_name <- paste("spark-apply-", as.numeric(Sys.time()), ".Rprof", sep = "")
      worker_log("starting new profile in ", file.path(getwd(), profile_name))
      utils::Rprof(profile_name)
    }

    if (config$debug) {
      worker_log("exiting to wait for debugging session to attach")

      # sleep for 1 day to allow long debugging sessions
      Sys.sleep(60*60*24)
      return()
    }

    worker_log("is starting")

    options(sparklyr.connection.cancellable = FALSE)

    sc <- spark_worker_connect(sessionId, backendPort, config)
    worker_log("is connected")

    spark_worker_apply(sc, config)

    if (identical(config$profile, TRUE)) {
      # utils::Rprof(NULL)
      worker_log("closing profile")
    }

  }, error = function(e) {
    worker_log_error("terminated unexpectedly: ", e$message)
    if (exists(".stopLastError", envir = .worker_globals)) {
      worker_log_error("collected callstack: \n", get(".stopLastError", envir = .worker_globals))
    }
    quit(status = -1)
  })

  worker_log("finished")
}

spark_worker_hooks <- function() {
  unlock <- get("unlockBinding")
  lock <- get("lockBinding")

  originalStop <- stop
  unlock("stop",  as.environment("package:base"))
  assign("stop", function(...) {
    frame_names <- list()
    frame_start <- max(1, sys.nframe() - 5)
    for (i in frame_start:sys.nframe()) {
      current_call <- sys.call(i)
      frame_names[[1 + i - frame_start]] <- paste(i, ": ", paste(head(deparse(current_call), 5), collapse = "\n"), sep = "")
    }

    assign(".stopLastError", paste(rev(frame_names), collapse = "\n"), envir = .worker_globals)
    originalStop(...)
  }, as.environment("package:base"))
  lock("stop",  as.environment("package:base"))
}

# nocov end
