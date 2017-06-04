spark_jobj <- function(x, ...) {
  UseMethod("spark_jobj")
}

spark_jobj.default <- function(x, ...) {
  stop("Unable to retrieve a spark_jobj from object of class ",
       paste(class(x), collapse = " "), call. = FALSE)
}

spark_jobj.spark_jobj <- function(x, ...) {
  x
}

print.spark_jobj <- function(x, ...) {
  print_jobj(worker_connection(x), x, ...)
}

print_jobj <- function(sc, jobj, ...) {
  UseMethod("print_jobj")
}

.validJobjs <- new.env(parent = emptyenv())

.toRemoveJobjs <- new.env(parent = emptyenv())

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
    class <- worker_invoke(jobj, "getClass")
    if (inherits(class, "spark_jobj"))
      class <- worker_invoke(class, "toString")
  }, error = function(e) {
  })
  tryCatch({
    repr <- worker_invoke(jobj, "toString")
  }, error = function(e) {
  })
  list(
    class = class,
    repr  = repr
  )
}

jobj_inspect <- function(jobj) {
  print(jobj)
  if (!connection_is_open(worker_connection(jobj)))
    return(jobj)

  class <- worker_invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- worker_invoke(class, "getDeclaredFields")
  lapply(fields, function(field) { print(field) })

  cat("Methods:\n")
  methods <- worker_invoke(class, "getDeclaredMethods")
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
