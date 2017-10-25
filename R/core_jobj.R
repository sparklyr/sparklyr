#' Retrieve a Spark JVM Object Reference
#'
#' This S3 generic is used for accessing the underlying Java Virtual Machine
#' (JVM) Spark objects associated with \R objects. These objects act as
#' references to Spark objects living in the JVM. Methods on these objects
#' can be called with the \code{\link{invoke}} family of functions.
#'
#' @param x An \R object containing, or wrapping, a \code{spark_jobj}.
#' @param ... Optional arguments; currently unused.
#'
#' @seealso \code{\link{invoke}}, for calling methods on Java object references.
#'
#' @export
spark_jobj <- function(x, ...) {
  UseMethod("spark_jobj")
}


#' @export
spark_jobj.default <- function(x, ...) {
  stop("Unable to retrieve a spark_jobj from object of class ",
       paste(class(x), collapse = " "), call. = FALSE)
}

#' @export
spark_jobj.spark_jobj <- function(x, ...) {
  x
}

#' @export
print.spark_jobj <- function(x, ...) {
  print_jobj(spark_connection(x), x, ...)
}

#' Generic method for print jobj for a connection type
#'
#' @param sc \code{spark_connection} (used for type dispatch)
#' @param jobj Object to print
#'
#' @keywords internal
#'
#' @export
print_jobj <- function(sc, jobj, ...) {
  UseMethod("print_jobj")
}


# Maintain a reference count of Java object references
# This allows us to GC the java object when it is safe
.validJobjs <- new.env(parent = emptyenv())

# List of object ids to be removed
.toRemoveJobjs <- new.env(parent = emptyenv())

# Check if jobj was created with the current SparkContext
isValidJobj <- function(jobj) {
  TRUE
}

getJobj <- function(con, objId) {
  newObj <- jobj_create(con, objId)
  if (exists(objId, .validJobjs)) {
    .validJobjs[[objId]] <- .validJobjs[[objId]] + 1
  } else {
    .validJobjs[[objId]] <- 1
  }
  newObj
}

jobj_subclass <- function(con) {
  UseMethod("jobj_subclass")
}

# Handler for a java object that exists on the backend.
jobj_create <- function(con, objId) {
  if (!is.character(objId)) {
    stop("object id must be a character")
  }
  # NOTE: We need a new env for a jobj as we can only register
  # finalizers for environments or external references pointers.
  obj <- structure(new.env(parent = emptyenv()), class = c("spark_jobj", jobj_subclass(con)))
  obj$id <- objId

  # Register a finalizer to remove the Java object when this reference
  # is garbage collected in R
  reg.finalizer(obj, cleanup.jobj)
  obj
}

jobj_info <- function(jobj) {
  if (!inherits(jobj, "spark_jobj"))
    stop("'jobj_info' called on non-jobj")

  class <- NULL
  repr <- NULL

  tryCatch({
    class <- invoke(jobj, "getClass")
    if (inherits(class, "spark_jobj"))
      class <- invoke(class, "getName")
  }, error = function(e) {
  })
  tryCatch({
    repr <- invoke(jobj, "toString")
  }, error = function(e) {
  })
  list(
    class = class,
    repr  = repr
  )
}

jobj_inspect <- function(jobj) {
  print(jobj)
  if (!connection_is_open(spark_connection(jobj)))
    return(jobj)

  class <- invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- invoke(class, "getDeclaredFields")
  lapply(fields, function(field) { print(field) })

  cat("Methods:\n")
  methods <- invoke(class, "getDeclaredMethods")
  lapply(methods, function(method) { print(method) })

  jobj
}

cleanup.jobj <- function(jobj) {
  if (isValidJobj(jobj)) {
    objId <- jobj$id
    # If we don't know anything about this jobj, ignore it
    if (exists(objId, envir = .validJobjs)) {
      .validJobjs[[objId]] <- .validJobjs[[objId]] - 1

      if (.validJobjs[[objId]] == 0) {
        rm(list = objId, envir = .validJobjs)
        # NOTE: We cannot call removeJObject here as the finalizer may be run
        # in the middle of another RPC. Thus we queue up this object Id to be removed
        # and then run all the removeJObject when the next RPC is called.
        .toRemoveJobjs[[objId]] <- 1
      }
    }
  }
}

clearJobjs <- function() {
  valid <- ls(.validJobjs)
  rm(list = valid, envir = .validJobjs)

  removeList <- ls(.toRemoveJobjs)
  rm(list = removeList, envir = .toRemoveJobjs)
}

attach_connection <- function(jobj, connection) {

  if (inherits(jobj, "spark_jobj")) {
    jobj$connection <- connection
  }
  else if (is.list(jobj) || inherits(jobj, "struct")) {
    jobj <- lapply(jobj, function(e) {
      attach_connection(e, connection)
    })
  }
  else if (is.environment(jobj)) {
    jobj <- eapply(jobj, function(e) {
      attach_connection(e, connection)
    })
  }

  jobj
}
