# nocov start

worker_invoke_method <- function(sc, static, object, method, ...) {
  core_invoke_method(sc, static, object, method, FALSE, ...)
}

worker_invoke <- function(jobj, method, ...) {
  UseMethod("worker_invoke")
}

#' @export
worker_invoke.shell_jobj <- function(jobj, method, ...) {
  worker_invoke_method(worker_connection(jobj), FALSE, jobj, method, ...)
}

worker_invoke_static <- function(sc, class, method, ...) {
  worker_invoke_method(sc, TRUE, class, method, ...)
}

worker_invoke_new <- function(sc, class, ...) {
  worker_invoke_method(sc, TRUE, class, "<init>", ...)
}

# nocov end
