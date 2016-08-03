
#' Execute a method on a remote Java object
#'
#' @param sc \code{spark_connection} to execute on.
#' @param jobj Java object to execute method on.
#' @param class Class to execute static method on.
#' @param method Name of method to execute.
#' @param ... Unused (future expansion)
#'
#' @export
invoke <- function (jobj, method, ...)
{
  invoke_method(spark_connection(jobj),
                FALSE,
                jobj,
                method,
                ...)
}


#' @name invoke
#' @export
invoke_static <- function (sc, class, method, ...)
{
  invoke_method(sc,
                TRUE,
                class,
                method,
                ...)
}


#' @name invoke
#' @export
invoke_new <- function(sc, class, ...)
{
  invoke_method(sc,
                TRUE,
                class,
                "<init>",
                ...)
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

