invoke_method <- function(sc, static, object, method, ...)
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
  if (length(returnStatus) == 0)
    stop("No status is returned. The sparklyr backend might have failed.")
  if (returnStatus != 0) {
    # get error message from backend and report to R
    msg <- readString(backend)
    withr::with_options(list(
      warning.length = 8000
    ), {
      if (nzchar(msg))
        stop(msg, call. = FALSE)
      else {
        # read the spark log
        msg <- read_spark_log_error(sc)
        stop(msg, call. = FALSE)
      }
    })
  }

  class(backend) <- c(class(backend), "shell_backend")

  object <- readObject(backend)
  attach_connection(object, sc)
}

worker_invoke <- function(jobj, method, ...) {
  UseMethod("worker_invoke")
}

worker_invoke.worker_jobj <- function(jobj, method, ...) {
  invoke_method(worker_connection(jobj), FALSE, jobj, method, ...)
}

worker_invoke_static <- function(sc, class, method, ...) {
  invoke_method(sc, TRUE, class, method, ...)
}

worker_invoke_new <- function(sc, class, ...) {
  invoke_method(sc, TRUE, class, "<init>", ...)
}
