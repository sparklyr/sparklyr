# nocov start

worker_log_env <- new.env()

worker_log_session <- function(sessionId) {
  assign('sessionId', sessionId, envir = worker_log_env)
}

worker_log_format <- function(message, level = "INFO", component = "RScript") {
  paste(
    format(Sys.time(), "%y/%m/%d %H:%M:%S"),
    " ",
    level,
    " sparklyr: ",
    component,
    " (",
    worker_log_env$sessionId,
    ") ",
    message,
    sep = "")
}

worker_log_level <- function(..., level) {
  if (is.null(worker_log_env$sessionId)) return()

  args = list(...)
  message <- paste(args, sep = "", collapse = "")
  formatted <- worker_log_format(message, level)
  cat(formatted, "\n")
}

worker_log <- function(...) {
  worker_log_level(..., level = "INFO")
}

worker_log_warning<- function(...) {
  worker_log_level(..., level = "WARN")
}

worker_log_error <- function(...) {
  worker_log_level(..., level = "ERROR")
}

# nocov end
