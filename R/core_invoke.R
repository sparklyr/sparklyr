core_invoke_method <- function(sc, static, object, method, ...)
{
  if (is.null(sc)) {
    stop("The connection is no longer valid.")
  }

  # if the object is a jobj then get it's id
  if (inherits(object, "spark_jobj"))
    object <- object$id

  rc <- rawConnection(raw(), "r+")
  writeString(rc, object)
  writeBoolean(rc, static)
  writeString(rc, method)

  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)

  backend <- sc$backend
  writeBin(con, backend)

  returnStatus <- readInt(backend)

  if (length(returnStatus) == 0) {
    # read the spark log
    msg <- core_read_spark_log_error(sc)
    close(sc$backend)
    close(sc$monitor)
    withr::with_options(list(
      warning.length = 8000
    ), {
      stop(
        "Unexpected state in sparklyr backend, terminating connection: ",
        msg,
        call. = FALSE)
    })
  }

  if (returnStatus != 0) {
    # get error message from backend and report to R
    msg <- readString(backend)
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

  class(backend) <- c(class(backend), "shell_backend")

  object <- readObject(backend)
  attach_connection(object, sc)
}

jobj_subclass.shell_backend <- function(con) {
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
