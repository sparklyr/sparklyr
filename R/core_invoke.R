core_invoke_sync_socket <- function(sc)
{
  flush <- c(1)
  while(length(flush) > 0) {
    flush <- readBin(sc$backend, raw(), 1000)

    # while flushing monitored connections we don't want to hang forever
    if (identical(sc$state$use_monitoring, TRUE)) break;
  }
}

core_invoke_sync <- function(sc)
{
  # sleep until connection clears is back on valid state
  while (!core_invoke_synced(sc)) {
    Sys.sleep(1)
    core_invoke_sync_socket(sc)
  }
}

core_invoke_cancel_running <- function(sc)
{
  if (is.null(spark_context(sc)))
    return()

  # if something fails while using a monitored connection we don't cancel jobs
  if (identical(sc$state$use_monitoring, TRUE))
    return()

  # if something fails while cancelling jobs we don't cancel jobs, this can
  # happen in OutOfMemory errors that shut down the spark context
  if (identical(sc$state$cancelling_all_jobs, TRUE))
    return()

  connection_progress_context(sc, function() {
    sc$state$cancelling_all_jobs <- TRUE
    on.exit(sc$state$cancelling_all_jobs <- FALSE)
    invoke(spark_context(sc), "cancelAllJobs")
  })

  if (exists("connection_progress_terminated")) connection_progress_terminated(sc)
}

write_bin_args <- function(backend, object, static, method, args) {
  rc <- rawConnection(raw(), "r+")
  writeString(rc, object)
  writeBoolean(rc, static)
  writeString(rc, method)

  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)

  writeBin(con, backend)
}

core_invoke_synced <- function(sc)
{
  if (is.null(sc))
    stop("The connection is no longer valid.")

  backend <- core_invoke_socket(sc)
  echo_id <- "sparklyr"

  write_bin_args(backend, "Handler", TRUE, "echo", echo_id)

  returnStatus <- readInt(backend)

  if (length(returnStatus) == 0 || returnStatus != 0) {
    FALSE
  }
  else {
    object <- readObject(sc)
    identical(object, echo_id)
  }
}

core_invoke_socket <- function(sc) {
  if (identical(sc$state$use_monitoring, TRUE))
    sc$monitoring
  else
    sc$backend
}

core_invoke_socket_name <- function(sc) {
  if (identical(sc$state$use_monitoring, TRUE))
    "monitoring"
  else
    "backend"
}

core_remove_jobjs <- function(sc, ids) {
  core_invoke_method_impl(sc, static = TRUE, noreply = TRUE, "Handler", "rm", as.list(ids))
}

core_invoke_method <- function(sc, static, object, method, ...)
{
  core_invoke_method_impl(sc, static, noreply = FALSE, object, method, ...)
}

core_invoke_method_impl <- function(sc, static, noreply, object, method, ...)
{
  # N.B.: the reference to `object` must be retained until after a value or exception is returned to us
  # from the invoked method here (i.e., cannot have `object <- something_else` before that), because any
  # re-assignment could cause the last reference to `object` to be destroyed and the underlying JVM object
  # to be deleted from JVMObjectTracker before the actual invocation of the method could happen.
  lockBinding("object", environment())

  if (is.null(sc))
    stop("The connection is no longer valid.")

  args <- list(...)

  # initialize status if needed
  if (is.null(sc$state$status))
    sc$state$status <- list()

  # choose connection socket
  backend <- core_invoke_socket(sc)
  connection_name <- core_invoke_socket_name(sc)

  if (!identical(object, "Handler")) {
    toRemoveJobjs <- sc$state$toRemoveJobjs
    objsToRemove <- ls(toRemoveJobjs)
    if (length(objsToRemove) > 0) {
      core_remove_jobjs(sc, objsToRemove)
      rm(list = objsToRemove, envir = toRemoveJobjs)
    }
  }

  if (!identical(object, "Handler") &&
      spark_config_value(sc$config, c("sparklyr.cancellable", "sparklyr.connection.cancellable"), TRUE)) {
    # if connection still running, sync to valid state
    if (identical(sc$state$status[[connection_name]], "running"))
      core_invoke_sync(sc)

    # while exiting this function, if interrupted (still running), cancel server job
    on.exit(core_invoke_cancel_running(sc))

    sc$state$status[[connection_name]] <- "running"
  }

  # if the object is a jobj then get it's id
  objId <- ifelse(inherits(object, "spark_jobj"), object$id, object)

  write_bin_args(backend, objId, static, method, args)

  if (identical(object, "Handler") &&
      (identical(method, "terminateBackend") || identical(method, "stopBackend"))) {
    # by the time we read response, backend might be already down.
    return(NULL)
  }

  result_object <- NULL
  if (!noreply) {
    # wait for a return status & result
    returnStatus <- readInt(sc)

    if (length(returnStatus) == 0) {
      # read the spark log
      msg <- core_read_spark_log_error(sc)

      withr::with_options(list(
        warning.length = 8000
      ), {
        stop(
          "Unexpected state in sparklyr backend: ",
          msg,
          call. = FALSE)
      })
    }

    if (returnStatus != 0) {
      # get error message from backend and report to R
      msg <- readString(sc)
      withr::with_options(list(
        warning.length = 8000
      ), {
        if (nzchar(msg)) {
          core_handle_known_errors(sc, msg)

          stop(msg, call. = FALSE)
        } else {
          # read the spark log
          msg <- core_read_spark_log_error(sc)
          stop(msg, call. = FALSE)
        }
      })
    }

    result_object <- readObject(sc)
  }

  sc$state$status[[connection_name]] <- "ready"
  on.exit(NULL)

  attach_connection(result_object, sc)
}

jobj_subclass.shell_backend <- function(con) {
  "shell_jobj"
}

jobj_subclass.spark_connection <- function(con) {
  "shell_jobj"
}

jobj_subclass.spark_worker_connection <- function(con) {
  "shell_jobj"
}

core_handle_known_errors <- function(sc, msg) {
  # Some systems might have an invalid hostname that Spark <= 2.0.1 fails to handle
  # gracefully and triggers unexpected errors such as #532. Under these versions,
  # we proactevely test getLocalHost() to warn users of this problem.
  if (grepl("ServiceConfigurationError.*tachyon", msg, ignore.case = TRUE)) {
    warning(
      "Failed to retrieve localhost, please validate that the hostname is correctly mapped. ",
      "Consider running `hostname` and adding that entry to your `/etc/hosts` file."
    )
  }
  else if (grepl("check worker logs for details", msg, ignore.case = TRUE) &&
           spark_master_is_local(sc$master)) {
    abort_shell(
      "sparklyr worker rscript failure, check worker logs for details",
      NULL, NULL, sc$output_file, sc$error_file)
  }
}

core_read_spark_log_error <- function(sc) {
  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- readLines(sc$output_file)
    splat <- strsplit(log, "\\s+", perl = TRUE)
    n <- length(splat)
    timestamp <- splat[[n]][[2]]
    regex <- paste("\\b", timestamp, "\\b", sep = "")
    entries <- grep(regex, log, perl = TRUE, value = TRUE)
    pasted <- paste(entries, collapse = "\n")
    msg <- paste("failed to invoke spark command", pasted, sep = "\n")
  })
  msg
}
