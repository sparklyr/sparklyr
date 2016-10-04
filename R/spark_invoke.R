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
  invoke_method(spark_connection(jobj), FALSE, jobj, method, ...)
}

#' @name invoke
#' @export
invoke_static <- function(sc, class, method, ...) {
  invoke_method(sc, TRUE, class, method, ...)
}

#' @name invoke
#' @export
invoke_new <- function(sc, class, ...) {
  invoke_method(sc, TRUE, class, "<init>", ...)
}

#' Generic call interface for spark shell
#'
#' @param sc \code{spark_connection}
#' @param static Is this a static method call (including a constructor). If so
#'   then the \code{object} parameter should be the name of a class (otherwise
#'   it should be a spark_jobj instance).
#' @param object Object instance or name of class (for \code{static})
#' @param method Name of method
#' @param ... Call parameters
#'
#' @keywords internal
#'
#' @export
invoke_method <- function(sc, static, object, method, ...) {
  UseMethod("invoke_method")
}

