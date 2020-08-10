# nocov start

connection_is_open.spark_worker_connection <- function(sc) {
  bothOpen <- FALSE
  if (!identical(sc, NULL)) {
    tryCatch(
      {
        bothOpen <- isOpen(sc$backend) && isOpen(sc$gateway)
      },
      error = function(e) {
      }
    )
  }
  bothOpen
}

worker_connection <- function(x, ...) {
  UseMethod("worker_connection")
}

worker_connection.spark_jobj <- function(x, ...) {
  x$connection
}

# nocov end
