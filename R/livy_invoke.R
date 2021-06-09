# nocov start

#' @import base64enc
livy_invoke_serialize <- function(sc, static, object, method, ...) {
  if (is.null(sc)) {
    stop("The connection is no longer valid.")
  }

  if (inherits(object, "livy_jobj")) {
    object <- object$id
  }

  rc <- rawConnection(raw(), "r+")
  writeString(rc, object)
  writeBoolean(rc, static)
  writeBoolean(rc, FALSE) # return_jobj_ref
  writeString(rc, method)

  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  base64 <- base64encode(bytes)
  base64
}

livy_invoke_deserialize <- function(sc, base64) {
  rv <- base64decode(base64)

  rc <- rawConnection(rv, "r+")

  returnStatus <- readInt(rc)
  if (length(returnStatus) == 0) {
    stop("No status is returned. Livy backend might have failed.")
  }
  if (returnStatus != 0) {
    msg <- readString(rc)
    withr::with_options(list(
      warning.length = 8000
    ), {
      close(rc)
      stop(msg, call. = FALSE)
    })
  }

  conn <- structure(list(rc = rc, state = sc$state), class = c("livy_backend"))

  object <- readObject(conn)
  close(rc)

  attach_connection(object, sc)
}

jobj_subclass.livy_backend <- function(con) {
  "livy_jobj"
}

# nocov end
