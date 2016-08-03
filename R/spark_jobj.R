# Imported from:
#    https://raw.githubusercontent.com/apache/spark/branch-1.6/R/pkg/R/jobj.R
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# References to objects that exist on the JVM backend
# are maintained using the jobj.


#' Get the spark_jobj associated with an object
#'
#' S3 method to get the spark_jobj associated with objects of
#' various types.
#'
#' @param x Object to extract jobj from
#' @param ... Reserved for future use
#' @return A \code{spark_jobj} object that can be passed to
#'   \code{\link{invoke}}.
#'
#' @seealso \code{\link{invoke}}
#'
#' @export
spark_jobj <- function(x, ...) {
  UseMethod("spark_jobj")
}


#' @export
spark_jobj.default <- function(x, ...) {
  stop("Unable to retreive a spark_jobj from object of class ",
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

getJobj <- function(objId) {
  newObj <- jobj_create(objId)
  if (exists(objId, .validJobjs)) {
    .validJobjs[[objId]] <- .validJobjs[[objId]] + 1
  } else {
    .validJobjs[[objId]] <- 1
  }
  newObj
}

# Handler for a java object that exists on the backend.
jobj_create <- function(objId) {
  if (!is.character(objId)) {
    stop("object id must be a character")
  }
  # NOTE: We need a new env for a jobj as we can only register
  # finalizers for environments or external references pointers.
  obj <- structure(new.env(parent = emptyenv()), class = "spark_jobj")
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
      class <- invoke(class, "toString")
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

