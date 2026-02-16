#' Invoke a Method on a JVM Object
#'
#' Invoke methods on Java object references. These functions provide a
#' mechanism for invoking various Java object methods directly from \R.
#'
#' Use each of these functions in the following scenarios:
#'
#' \tabular{lll}{
#' \code{invoke} \tab Execute a method on a Java object reference (typically, a \code{spark_jobj}). \cr
#' \code{invoke_static} \tab Execute a static method associated with a Java class. \cr
#' \code{invoke_new} \tab Invoke a constructor associated with a Java class. \cr
#' }
#'
#' @param sc A \code{spark_connection}.
#' @param jobj An \R object acting as a Java object reference (typically, a \code{spark_jobj}).
#' @param class The name of the Java class whose methods should be invoked.
#' @param method The name of the method to be invoked.
#' @param ... Optional arguments, currently unused.
#'
#' @name invoke
NULL

#' @name invoke
#' @export
invoke <- function(jobj, method, ...) {
  invoke_trace(spark_connection(jobj), "Invoking", method)
  UseMethod("invoke")
}

#' Invoke a Java function.
#'
#' Invoke a Java function and force return value of the call to be retrieved
#' as a Java object reference.
#'
#' @inheritParams invoke
#'
#' @name j_invoke
NULL

#' @name j_invoke
#' @export
j_invoke <- function(jobj, method, ...) {
  invoke_trace(spark_connection(jobj), "Invoking", method)
  UseMethod("j_invoke")
}

#' @name invoke
#' @export
invoke_static <- function(sc, class, method, ...) {
  invoke_trace(sc, "Invoking", class, method)
  UseMethod("invoke_static")
}

#' @name j_invoke
#' @export
j_invoke_static <- function(sc, class, method, ...) {
  UseMethod("j_invoke_static")
}

#' @name invoke
#' @export
invoke_new <- function(sc, class, ...) {
  invoke_trace(sc, "Invoking", class)
  UseMethod("invoke_new")
}

#' @name j_invoke
#' @export
j_invoke_new <- function(sc, class, ...) {
  UseMethod("j_invoke_new")
}

#' Generic Call Interface
#'
#' @param sc \code{spark_connection}
#' @param static Is this a static method call (including a constructor). If so
#'   then the \code{object} parameter should be the name of a class (otherwise
#'   it should be a spark_jobj instance).
#' @param object Object instance or name of class (for \code{static})
#' @param method Name of method
#' @param ... Call parameters
#'
#' @name generic_call_interface
NULL

#' Generic Call Interface
#'
#' @inheritParams generic_call_interface
#'
#' @keywords internal
#'
#' @export
invoke_method <- function(sc, static, object, method, ...) {
  UseMethod("invoke_method")
}

#' Generic Call Interface
#'
#' Call a Java method and retrieve the return value through a JVM object
#' reference.
#'
#' @inheritParams generic_call_interface
#'
#' @keywords internal
#'
#' @export
j_invoke_method <- function(sc, static, object, method, ...) {
  UseMethod("j_invoke_method")
}

invoke_trace <- function(sc, ...) {
  invoke_config <- spark_config_value(sc$config, "sparklyr.log.invoke", FALSE)
  if (invoke_config %in% c(TRUE, "callstack", "cat")) {
    args <- list(...)
    trace_message <- paste(args, collapse = " ")

    if (identical(invoke_config, "cat")) cat(paste0(trace_message, "\n")) else message(trace_message)

    if (identical(invoke_config, "callstack")) {
      frame_names <- list()
      for (i in 1:sys.nframe()) {
        current_call <- sys.call(i)
        frame_names[[i]] <- paste(i, ": ", paste(head(deparse(current_call), 5), collapse = "\n"), sep = "")
      }

      message(paste(frame_names, collapse = "\n"))
      message()
    }
  }
}
