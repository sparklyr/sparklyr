# Changing this file requires running update_embedded_sources.R to rebuild sources and jars.

arrow_write_record_batch <- function(df, spark_version_number = NULL) {
  arrow_env_vars <- list()
  if (!is.null(spark_version_number) && spark_version_number < "3.0") {
    # Spark < 3 uses an old version of Arrow, so send data in the legacy format
    arrow_env_vars$ARROW_PRE_0_15_IPC_FORMAT <- 1
  }

  withr::with_envvar(arrow_env_vars, {
    # New in arrow 0.17: takes a data.frame and returns a raw buffer with Arrow data
    if ("write_to_raw" %in% ls(envir = asNamespace("arrow"))) {
      # Fixed in 0.17: arrow doesn't hardcode a GMT timezone anymore
      # so set the local timezone to any POSIXt columns that don't have one set
      # https://github.com/sparklyr/sparklyr/issues/2439
      df[] <- lapply(df, function(x) {
        if (inherits(x, "POSIXt") && is.null(attr(x, "tzone"))) {
          attr(x, "tzone") <- Sys.timezone()
        }
        x
      })
      arrow::write_to_raw(df, format = "stream")
    } else {
      arrow::write_arrow(arrow::record_batch(!!!df), raw())
    }
  })
}

arrow_record_stream_reader <- function(stream) {
  arrow::RecordBatchStreamReader$create(stream)
}

arrow_read_record_batch <- function(reader) reader$read_next_batch()

arrow_as_tibble <- function(record) as.data.frame(record)
#' A helper function to retrieve values from \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_value <- function(config, name, default = NULL) {
  if (getOption("sparklyr.test.enforce.config", FALSE) && any(grepl("^sparklyr.", name))) {
    settings <- get("spark_config_settings")()
    if (!any(name %in% settings$name) &&
      !grepl("^sparklyr\\.shell\\.", name)) {
      stop("Config value '", name[[1]], "' not described in spark_config_settings()")
    }
  }

  name_exists <- name %in% names(config)
  if (!any(name_exists)) {
    name_exists <- name %in% names(options())
    if (!any(name_exists)) {
      value <- default
    } else {
      name_primary <- name[name_exists][[1]]
      value <- getOption(name_primary)
    }
  } else {
    name_primary <- name[name_exists][[1]]
    value <- config[[name_primary]]
  }

  if (is.language(value)) value <- rlang::as_closure(value)
  if (is.function(value)) value <- value()
  value
}

spark_config_integer <- function(config, name, default = NULL) {
  as.integer(spark_config_value(config, name, default))
}

spark_config_logical <- function(config, name, default = NULL) {
  as.logical(spark_config_value(config, name, default))
}
#' Check whether the connection is open
#'
#' @param sc \code{spark_connection}
#'
#' @keywords internal
#'
#' @export
connection_is_open <- function(sc) {
  UseMethod("connection_is_open")
}
read_bin <- function(con, what, n, endian = NULL) {
  UseMethod("read_bin")
}

read_bin.default <- function(con, what, n, endian = NULL) {
  if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)
}

read_bin_wait <- function(con, what, n, endian = NULL) {
  sc <- con
  con <- if (!is.null(sc$state) && identical(sc$state$use_monitoring, TRUE)) sc$monitoring else sc$backend

  timeout <- spark_config_value(sc$config, "sparklyr.backend.timeout", 30 * 24 * 60 * 60)
  progressInterval <- spark_config_value(sc$config, "sparklyr.progress.interval", 3)

  result <- if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)

  progressTimeout <- Sys.time() + progressInterval
  if (is.null(sc$state$progress)) {
    sc$state$progress <- new.env()
  }
  progressUpdated <- FALSE

  waitInterval <- 0
  commandStart <- Sys.time()
  while (length(result) == 0 && commandStart + timeout > Sys.time()) {
    Sys.sleep(waitInterval)
    waitInterval <- min(0.1, waitInterval + 0.01)

    result <- if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)

    if (Sys.time() > progressTimeout) {
      progressTimeout <- Sys.time() + progressInterval
      if (exists("connection_progress")) {
        connection_progress(sc)
        progressUpdated <- TRUE
      }
    }
  }

  if (progressUpdated) connection_progress_terminated(sc)

  if (commandStart + timeout <= Sys.time()) {
    stop("Operation timed out, increase config option sparklyr.backend.timeout if needed.")
  }

  result
}

read_bin.spark_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

read_bin.spark_worker_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

read_bin.livy_backend <- function(con, what, n, endian = NULL) {
  read_bin.default(con$rc, what, n, endian)
}

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch(type,
    "i" = readInt(con),
    "c" = readString(con),
    "b" = readBoolean(con),
    "d" = readDouble(con),
    "r" = readRaw(con),
    "D" = readDate(con),
    "t" = readTime(con),
    "a" = readArray(con),
    "l" = readList(con),
    "e" = readMap(con),
    "s" = readStruct(con),
    "f" = readFastStringArray(con),
    "n" = NULL,
    "j" = getJobj(con, readString(con)),
    "J" = jsonlite::fromJSON(
      readString(con),
      simplifyDataFrame = FALSE, simplifyMatrix = FALSE
    ),
    stop(paste("Unsupported type for deserialization", type))
  )
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- ""

  if (stringLen > 0) {
    raw <- read_bin(con, raw(), stringLen, endian = "big")
    if (is.element("00", raw)) {
      warning("Input contains embedded nuls, removing.")
      raw <- raw[raw != "00"]
    }
    string <- rawToChar(raw)
  }

  Encoding(string) <- "UTF-8"
  string
}

readFastStringArray <- function(con) {
  joined <- readString(con)
  as.list(strsplit(joined, "\u0019")[[1]])
}

readDateArray <- function(con, n = 1) {
  if (n == 0) {
    as.Date(NA)
  } else {
    do.call(c, lapply(seq(n), function(x) readDate(con)))
  }
}

readInt <- function(con, n = 1) {
  if (n == 0) {
    integer(0)
  } else {
    read_bin(con, integer(), n = n, endian = "big")
  }
}

readDouble <- function(con, n = 1) {
  if (n == 0) {
    double(0)
  } else {
    read_bin(con, double(), n = n, endian = "big")
  }
}

readBoolean <- function(con, n = 1) {
  if (n == 0) {
    logical(0)
  } else {
    as.logical(readInt(con, n = n))
  }
}

readType <- function(con) {
  rawToChar(read_bin(con, "raw", n = 1L))
}

readDate <- function(con) {
  n <- readInt(con)
  if (is.na(n)) {
    as.Date(NA)
  } else {
    d <- as.Date(n, origin = "1970-01-01", tz = "UTC")
    if (getOption("sparklyr.collect.datechars", FALSE)) {
      as.character(d)
    } else {
      d
    }
  }
}

readTime <- function(con, n = 1) {
  if (identical(n, 0)) {
    as.POSIXct(character(0))
  } else {
    t <- readDouble(con, n)

    r <- as.POSIXct(t, origin = "1970-01-01", tz = "UTC")
    if (getOption("sparklyr.collect.datechars", FALSE)) {
      as.character(r)
    } else {
      r
    }
  }
}

readArray <- function(con) {
  type <- readType(con)
  len <- readInt(con)

  if (type == "d") {
    return(readDouble(con, n = len))
  } else if (type == "i") {
    return(readInt(con, n = len))
  } else if (type == "b") {
    return(readBoolean(con, n = len))
  } else if (type == "t") {
    return(readTime(con, n = len))
  } else if (type == "D") {
    return(readDateArray(con, n = len))
  }

  if (len > 0) {
    l <- vector("list", len)
    for (i in seq_len(len)) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
  } else {
    list()
  }
}

# Read a list. Types of each element may be different.
# Null objects are read as NA.
readList <- function(con) {
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in seq_len(len)) {
      elem <- readObject(con)
      if (is.null(elem)) {
        elem <- NA
      }
      l[[i]] <- elem
    }
    l
  } else {
    list()
  }
}

readMap <- function(con) {
  map <- list()
  len <- readInt(con)
  if (len > 0) {
    for (i in seq_len(len)) {
      key <- readString(con)
      value <- readObject(con)
      map[[key]] <- value
    }
  }

  map
}

# Convert a named list to struct so that
# SerDe won't confuse between a normal named list and struct
listToStruct <- function(list) {
  stopifnot(class(list) == "list")
  stopifnot(!is.null(names(list)))
  class(list) <- "struct"
  list
}

# Read a field of StructType from DataFrame
# into a named list in R whose class is "struct"
readStruct <- function(con) {
  names <- readObject(con)
  fields <- readObject(con)
  names(fields) <- names
  listToStruct(fields)
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  if (dataLen == 0) {
    raw()
  } else {
    read_bin(con, raw(), as.integer(dataLen), endian = "big")
  }
}
sparklyr_gateway_trouble_shooting_msg <- function() {
  c(
    "\n\n\nTry running `options(sparklyr.log.console = TRUE)` followed by ",
    "`sc <- spark_connect(...)` for more debugging info."
  )
}

wait_connect_gateway <- function(gatewayAddress, gatewayPort, config, isStarting) {
  waitSeconds <- if (isStarting) {
    spark_config_value(config, "sparklyr.connect.timeout", 60)
  } else {
    spark_config_value(config, "sparklyr.gateway.timeout", 1)
  }

  gateway <- NULL
  commandStart <- Sys.time()

  while (is.null(gateway) && Sys.time() < commandStart + waitSeconds) {
    tryCatch(
      {
        suppressWarnings({
          timeout <- spark_config_value(config, "sparklyr.gateway.interval", 1)
          gateway <- socketConnection(
            host = gatewayAddress,
            port = gatewayPort,
            server = FALSE,
            blocking = TRUE,
            open = "rb",
            timeout = timeout
          )
        })
      },
      error = function(err) {
      }
    )

    startWait <- spark_config_value(config, "sparklyr.gateway.wait", 50 / 1000)
    Sys.sleep(startWait)
  }

  gateway
}

spark_gateway_commands <- function() {
  list(
    "GetPorts" = 0,
    "RegisterInstance" = 1
  )
}

query_gateway_for_port <- function(gateway, sessionId, config, isStarting) {
  waitSeconds <- if (isStarting) {
    spark_config_value(config, "sparklyr.connect.timeout", 60)
  } else {
    spark_config_value(config, "sparklyr.gateway.timeout", 1)
  }

  writeInt(gateway, spark_gateway_commands()[["GetPorts"]])
  writeInt(gateway, sessionId)
  writeInt(gateway, if (isStarting) waitSeconds else 0)

  backendSessionId <- NULL
  redirectGatewayPort <- NULL

  commandStart <- Sys.time()
  while (length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()) {
    backendSessionId <- readInt(gateway)
    Sys.sleep(0.1)
  }

  redirectGatewayPort <- readInt(gateway)
  backendPort <- readInt(gateway)

  if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {
    if (isStarting) {
      stop(
        "Sparklyr gateway did not respond while retrieving ports information after ",
        waitSeconds,
        " seconds.",
        sparklyr_gateway_trouble_shooting_msg()
      )
    } else {
      return(NULL)
    }
  }

  list(
    gateway = gateway,
    backendPort = backendPort,
    redirectGatewayPort = redirectGatewayPort
  )
}

spark_connect_gateway <- function(
                                  gatewayAddress,
                                  gatewayPort,
                                  sessionId,
                                  config,
                                  isStarting = FALSE) {

  # try connecting to existing gateway
  gateway <- wait_connect_gateway(gatewayAddress, gatewayPort, config, isStarting)

  if (is.null(gateway)) {
    if (isStarting) {
      stop(
        "Gateway in ", gatewayAddress, ":", gatewayPort, " did not respond.",
        sparklyr_gateway_trouble_shooting_msg()
      )
    }

    NULL
  }
  else {
    worker_log("is querying ports from backend using port ", gatewayPort)

    gateway_ports_query_attempts <- as.integer(
      spark_config_value(config, "sparklyr.gateway.port.query.attempts", 3L)
    )
    gateway_ports_query_retry_interval_s <- as.integer(
      spark_config_value(config, "sparklyr.gateway.port.query.retry.interval.seconds", 4L)
    )
    while (gateway_ports_query_attempts > 0) {
      gateway_ports_query_attempts <- gateway_ports_query_attempts - 1
      withCallingHandlers(
        {
          gatewayPortsQuery <- query_gateway_for_port(
            gateway,
            sessionId,
            config,
            isStarting
          )
          break
        },
        error = function(e) {
          isStarting <- FALSE
          if (gateway_ports_query_attempts > 0) {
            Sys.sleep(gateway_ports_query_retry_interval_s)
          }
          NULL
        }
      )
    }
    if (is.null(gatewayPortsQuery) && !isStarting) {
      close(gateway)
      return(NULL)
    }

    redirectGatewayPort <- gatewayPortsQuery$redirectGatewayPort
    backendPort <- gatewayPortsQuery$backendPort

    worker_log("found redirect gateway port ", redirectGatewayPort)

    if (redirectGatewayPort == 0) {
      close(gateway)

      if (isStarting) {
        stop("Gateway in ", gatewayAddress, ":", gatewayPort, " does not have the requested session registered")
      }

      NULL
    } else if (redirectGatewayPort != gatewayPort) {
      close(gateway)

      spark_connect_gateway(gatewayAddress, redirectGatewayPort, sessionId, config, isStarting)
    }
    else {
      list(
        gateway = gateway,
        backendPort = backendPort
      )
    }
  }
}
core_invoke_sync_socket <- function(sc) {
  flush <- c(1)
  while (length(flush) > 0) {
    flush <- readBin(sc$backend, raw(), 1000)

    # while flushing monitored connections we don't want to hang forever
    if (identical(sc$state$use_monitoring, TRUE)) break
  }
}

core_invoke_sync <- function(sc) {
  # sleep until connection clears is back on valid state
  while (!core_invoke_synced(sc)) {
    Sys.sleep(1)
    core_invoke_sync_socket(sc)
  }
}

core_invoke_cancel_running <- function(sc) {
  if (is.null(spark_context(sc))) {
    return()
  }

  # if something fails while using a monitored connection we don't cancel jobs
  if (identical(sc$state$use_monitoring, TRUE)) {
    return()
  }

  # if something fails while cancelling jobs we don't cancel jobs, this can
  # happen in OutOfMemory errors that shut down the spark context
  if (identical(sc$state$cancelling_all_jobs, TRUE)) {
    return()
  }

  connection_progress_context(sc, function() {
    sc$state$cancelling_all_jobs <- TRUE
    on.exit(sc$state$cancelling_all_jobs <- FALSE)
    invoke(spark_context(sc), "cancelAllJobs")
  })

  if (exists("connection_progress_terminated")) connection_progress_terminated(sc)
}

write_bin_args <- function(backend, object, static, method, args) {
  rc <- rawConnection(raw(), "r+")
  writeString(rc, object)
  writeBoolean(rc, static)
  writeString(rc, method)

  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)

  writeBin(con, backend)
}

core_invoke_synced <- function(sc) {
  if (is.null(sc)) {
    stop("The connection is no longer valid.")
  }

  backend <- core_invoke_socket(sc)
  echo_id <- "sparklyr"

  write_bin_args(backend, "Handler", TRUE, "echo", echo_id)

  returnStatus <- readInt(backend)

  if (length(returnStatus) == 0 || returnStatus != 0) {
    FALSE
  }
  else {
    object <- readObject(sc)
    identical(object, echo_id)
  }
}

core_invoke_socket <- function(sc) {
  if (identical(sc$state$use_monitoring, TRUE)) {
    sc$monitoring
  } else {
    sc$backend
  }
}

core_invoke_socket_name <- function(sc) {
  if (identical(sc$state$use_monitoring, TRUE)) {
    "monitoring"
  } else {
    "backend"
  }
}

core_remove_jobjs <- function(sc, ids) {
  core_invoke_method_impl(sc, static = TRUE, noreply = TRUE, "Handler", "rm", as.list(ids))
}

core_invoke_method <- function(sc, static, object, method, ...) {
  core_invoke_method_impl(sc, static, noreply = FALSE, object, method, ...)
}

core_invoke_method_impl <- function(sc, static, noreply, object, method, ...) {
  # N.B.: the reference to `object` must be retained until after a value or exception is returned to us
  # from the invoked method here (i.e., cannot have `object <- something_else` before that), because any
  # re-assignment could cause the last reference to `object` to be destroyed and the underlying JVM object
  # to be deleted from JVMObjectTracker before the actual invocation of the method could happen.
  lockBinding("object", environment())

  if (is.null(sc)) {
    stop("The connection is no longer valid.")
  }

  args <- list(...)

  # initialize status if needed
  if (is.null(sc$state$status)) {
    sc$state$status <- list()
  }

  # choose connection socket
  backend <- core_invoke_socket(sc)
  connection_name <- core_invoke_socket_name(sc)

  if (!identical(object, "Handler")) {
    toRemoveJobjs <- get_to_remove_jobjs(sc)
    objsToRemove <- ls(toRemoveJobjs)
    if (length(objsToRemove) > 0) {
      core_remove_jobjs(sc, objsToRemove)
      rm(list = objsToRemove, envir = toRemoveJobjs)
    }
  }

  if (!identical(object, "Handler") &&
    spark_config_value(sc$config, c("sparklyr.cancellable", "sparklyr.connection.cancellable"), TRUE)) {
    # if connection still running, sync to valid state
    if (identical(sc$state$status[[connection_name]], "running")) {
      core_invoke_sync(sc)
    }

    # while exiting this function, if interrupted (still running), cancel server job
    on.exit(core_invoke_cancel_running(sc))

    sc$state$status[[connection_name]] <- "running"
  }

  # if the object is a jobj then get it's id
  objId <- ifelse(inherits(object, "spark_jobj"), object$id, object)

  write_bin_args(backend, objId, static, method, args)

  if (identical(object, "Handler") &&
    (identical(method, "terminateBackend") || identical(method, "stopBackend"))) {
    # by the time we read response, backend might be already down.
    return(NULL)
  }

  result_object <- NULL
  if (!noreply) {
    # wait for a return status & result
    returnStatus <- readInt(sc)

    if (length(returnStatus) == 0) {
      # read the spark log
      msg <- core_read_spark_log_error(sc)

      withr::with_options(list(
        warning.length = 8000
      ), {
        stop(
          "Unexpected state in sparklyr backend: ",
          msg,
          call. = FALSE
        )
      })
    }

    if (returnStatus != 0) {
      # get error message from backend and report to R
      msg <- readString(sc)
      withr::with_options(list(
        warning.length = 8000
      ), {
        if (nzchar(msg)) {
          core_handle_known_errors(sc, msg)

          stop(msg, call. = FALSE)
        } else {
          # read the spark log
          msg <- core_read_spark_log_error(sc)
          stop(msg, call. = FALSE)
        }
      })
    }

    result_object <- readObject(sc)
  }

  sc$state$status[[connection_name]] <- "ready"
  on.exit(NULL)

  attach_connection(result_object, sc)
}

jobj_subclass.shell_backend <- function(con) {
  "shell_jobj"
}

jobj_subclass.spark_connection <- function(con) {
  "shell_jobj"
}

jobj_subclass.spark_worker_connection <- function(con) {
  "shell_jobj"
}

core_handle_known_errors <- function(sc, msg) {
  # Some systems might have an invalid hostname that Spark <= 2.0.1 fails to handle
  # gracefully and triggers unexpected errors such as #532. Under these versions,
  # we proactevely test getLocalHost() to warn users of this problem.
  if (grepl("ServiceConfigurationError.*tachyon", msg, ignore.case = TRUE)) {
    warning(
      "Failed to retrieve localhost, please validate that the hostname is correctly mapped. ",
      "Consider running `hostname` and adding that entry to your `/etc/hosts` file."
    )
  }
  else if (grepl("check worker logs for details", msg, ignore.case = TRUE) &&
    spark_master_is_local(sc$master)) {
    abort_shell(
      "sparklyr worker rscript failure, check worker logs for details",
      NULL, NULL, sc$output_file, sc$error_file
    )
  }
}

core_read_spark_log_error <- function(sc) {
  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- readLines(sc$output_file)
    splat <- strsplit(log, "\\s+", perl = TRUE)
    n <- length(splat)
    timestamp <- splat[[n]][[2]]
    regex <- paste("\\b", timestamp, "\\b", sep = "")
    entries <- grep(regex, log, perl = TRUE, value = TRUE)
    pasted <- paste(entries, collapse = "\n")
    msg <- paste("failed to invoke spark command", pasted, sep = "\n")
  })
  msg
}
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
#' @exportClass spark_jobj
#' @export
spark_jobj <- function(x, ...) {
  UseMethod("spark_jobj")
}

spark_jobj_id <- function(x) {
  x$id
}

#' @export
spark_jobj.default <- function(x, ...) {
  stop("Unable to retrieve a spark_jobj from object of class ",
    paste(class(x), collapse = " "),
    call. = FALSE
  )
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

get_valid_jobjs <- function(con) {
  if (is.null(con$state$validJobjs)) {
    con$state$validJobjs <- new.env(parent = emptyenv())
  }
  con$state$validJobjs
}

get_to_remove_jobjs <- function(con) {
  if (is.null(con$state$toRemoveJobjs)) {
    con$state$toRemoveJobjs <- new.env(parent = emptyenv())
  }
  con$state$toRemoveJobjs
}

# Check if jobj points to a valid external JVM object
isValidJobj <- function(jobj) {
  exists("connection", jobj) && exists(jobj$id, get_valid_jobjs(jobj$connection))
}

getJobj <- function(con, objId) {
  newObj <- jobj_create(con, objId)
  validJobjs <- get_valid_jobjs(con)
  validJobjs[[objId]] <- get0(objId, validJobjs, ifnotfound = 0) + 1

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
  if (!inherits(jobj, "spark_jobj")) {
    stop("'jobj_info' called on non-jobj")
  }

  class <- NULL
  repr <- NULL

  tryCatch(
    {
      class <- invoke(jobj, "getClass")
      if (inherits(class, "spark_jobj")) {
        class <- invoke(class, "getName")
      }
    },
    error = function(e) {
    }
  )
  tryCatch(
    {
      repr <- invoke(jobj, "toString")
    },
    error = function(e) {
    }
  )
  list(
    class = class,
    repr = repr
  )
}

jobj_inspect <- function(jobj) {
  print(jobj)
  if (!connection_is_open(spark_connection(jobj))) {
    return(jobj)
  }

  class <- invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- invoke(class, "getDeclaredFields")
  lapply(fields, function(field) {
    print(field)
  })

  cat("Methods:\n")
  methods <- invoke(class, "getDeclaredMethods")
  lapply(methods, function(method) {
    print(method)
  })

  jobj
}

cleanup.jobj <- function(jobj) {
  if (isValidJobj(jobj)) {
    objId <- jobj$id
    validJobjs <- get_valid_jobjs(jobj$connection)
    validJobjs[[objId]] <- validJobjs[[objId]] - 1

    if (validJobjs[[objId]] == 0) {
      rm(list = objId, envir = validJobjs)
      # NOTE: We cannot call removeJObject here as the finalizer may be run
      # in the middle of another RPC. Thus we queue up this object Id to be removed
      # and then run all the removeJObject when the next RPC is called.
      toRemoveJobjs <- get_to_remove_jobjs(jobj$connection)
      toRemoveJobjs[[objId]] <- 1
    }
  }
}

clear_jobjs <- function() {
  scons <- spark_connection_find()
  for (scon in scons) {
    validJobjs <- get_valid_jobjs(scons)
    valid <- ls(validJobjs)
    rm(list = valid, envir = validJobjs)

    toRemoveJobjs <- get_to_remove_jobjs(scons)
    removeList <- ls(toRemoveJobjs)
    rm(list = removeList, envir = toRemoveJobjs)
  }
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
# Utility functions to serialize R objects so they can be read in Java.

# nolint start
# Type mapping from R to Java
#
# NULL -> Void
# integer -> Int
# character -> String
# logical -> Boolean
# double, numeric -> Double
# raw -> Array[Byte]
# Date -> Date
# POSIXct,POSIXlt -> Timestamp
#
# list[T] -> Array[T], where T is one of above mentioned types
# environment -> Map[String, T], where T is a native type
# jobj -> Object, where jobj is an object created in the backend
# nolint end

getSerdeType <- function(object) {
  type <- class(object)[[1]]

  if (type != "list") {
    type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) {
      getSerdeType(elem)
    }))
    if (length(elemType) <= 1) {

      # Check that there are no NAs in arrays since they are unsupported in scala
      hasNAs <- any(is.na(object))

      if (hasNAs) {
        "list"
      } else {
        "array"
      }
    } else {
      "list"
    }
  }
}

writeObject <- function(con, object, writeType = TRUE) {
  type <- class(object)[[1]]

  if (type %in% c("integer", "character", "logical", "double", "numeric", "factor", "Date", "POSIXct")) {
    if (is.na(object)) {
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(serdeType,
    NULL = writeVoid(con),
    integer = writeInt(con, object),
    character = writeString(con, object),
    logical = writeBoolean(con, object),
    double = writeDouble(con, object),
    numeric = writeDouble(con, object),
    raw = writeRaw(con, object),
    array = writeArray(con, object),
    list = writeList(con, object),
    struct = writeList(con, object),
    spark_jobj = writeJobj(con, object),
    environment = writeEnv(con, object),
    Date = writeDate(con, object),
    POSIXlt = writeTime(con, object),
    POSIXct = writeTime(con, object),
    factor = writeFactor(con, object),
    `data.frame` = writeList(con, object),
    spark_apply_binary_result = writeList(con, object),
    stop("Unsupported type '", serdeType, "' for serialization")
  )
}

writeVoid <- function(con) {
  # no value for NULL
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(as.character(value))
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

writeType <- function(con, class) {
  type <- switch(class,
    NULL = "n",
    integer = "i",
    character = "c",
    logical = "b",
    double = "d",
    numeric = "d",
    raw = "r",
    array = "a",
    list = "l",
    struct = "s",
    spark_jobj = "j",
    environment = "e",
    Date = "D",
    POSIXlt = "t",
    POSIXct = "t",
    factor = "c",
    `data.frame` = "l",
    spark_apply_binary_result = "l",
    stop("Unsupported type '", class, "' for serialization")
  )
  writeBin(charToRaw(type), con)
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) {
      env[[x]]
    })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeInt(con, as.integer(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}

writeFactor <- function(con, factor) {
  writeString(con, as.character(factor))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}
core_get_package_function <- function(packageName, functionName) {
  if (packageName %in% rownames(installed.packages()) &&
    exists(functionName, envir = asNamespace(packageName))) {
    get(functionName, envir = asNamespace(packageName))
  } else {
    NULL
  }
}
worker_config_serialize <- function(config) {
  paste(
    if (isTRUE(config$debug)) "TRUE" else "FALSE",
    spark_config_value(config, "sparklyr.worker.gateway.port", "8880"),
    spark_config_value(config, "sparklyr.worker.gateway.address", "localhost"),
    if (isTRUE(config$profile)) "TRUE" else "FALSE",
    if (isTRUE(config$schema)) "TRUE" else "FALSE",
    if (isTRUE(config$arrow)) "TRUE" else "FALSE",
    if (isTRUE(config$fetch_result_as_sdf)) "TRUE" else "FALSE",
    if (isTRUE(config$single_binary_column)) "TRUE" else "FALSE",
    config$spark_version,
    sep = ";"
  )
}

worker_config_deserialize <- function(raw) {
  parts <- strsplit(raw, ";")[[1]]

  list(
    debug = as.logical(parts[[1]]),
    sparklyr.gateway.port = as.integer(parts[[2]]),
    sparklyr.gateway.address = parts[[3]],
    profile = as.logical(parts[[4]]),
    schema = as.logical(parts[[5]]),
    arrow = as.logical(parts[[6]]),
    fetch_result_as_sdf = as.logical(parts[[7]]),
    single_binary_column = as.logical(parts[[8]]),
    spark_version = parts[[9]]
  )
}
# nocov start

spark_worker_context <- function(sc) {
  hostContextId <- worker_invoke_method(sc, FALSE, "Handler", "getHostContext")
  worker_log("retrieved worker context id ", hostContextId)

  context <- structure(
    class = c("spark_jobj", "shell_jobj"),
    list(
      id = hostContextId,
      connection = sc
    )
  )

  worker_log("retrieved worker context")

  context
}

spark_worker_init_packages <- function(sc, context) {
  bundlePath <- worker_invoke(context, "getBundlePath")

  if (nchar(bundlePath) > 0) {
    bundleName <- basename(bundlePath)
    worker_log("using bundle name ", bundleName)

    workerRootDir <- worker_invoke_static(sc, "org.apache.spark.SparkFiles", "getRootDirectory")
    sparkBundlePath <- file.path(workerRootDir, bundleName)

    worker_log("using bundle path ", normalizePath(sparkBundlePath))

    if (!file.exists(sparkBundlePath)) {
      stop("failed to find bundle under SparkFiles root directory")
    }

    unbundlePath <- worker_spark_apply_unbundle(
      sparkBundlePath,
      workerRootDir,
      tools::file_path_sans_ext(bundleName)
    )

    .libPaths(unbundlePath)
    worker_log("updated .libPaths with bundle packages")
  }
  else {
    spark_env <- worker_invoke_static(sc, "org.apache.spark.SparkEnv", "get")
    spark_libpaths <- worker_invoke(worker_invoke(spark_env, "conf"), "get", "spark.r.libpaths", NULL)
    if (!is.null(spark_libpaths)) {
      spark_libpaths <- unlist(strsplit(spark_libpaths, split = ","))
      .libPaths(spark_libpaths)
    }
  }
}

spark_worker_execute_closure <- function(
                                         closure,
                                         df,
                                         funcContext,
                                         grouped_by,
                                         barrier_map,
                                         fetch_result_as_sdf,
                                         partition_index) {
  if (nrow(df) == 0) {
    worker_log("found that source has no rows to be proceesed")
    return(NULL)
  }

  barrier_arg <- NULL
  worker_log("barrier is ", as.character(barrier_map))
  if (length(barrier_map) > 0) {
    worker_log("found barrier execution context")
    barrier_arg <- list(barrier = barrier_map)
  }

  closure_params <- length(formals(closure))
  has_partition_index_param <- (
    !is.null(funcContext$partition_index_param) &&
      nchar(funcContext$partition_index_param) > 0
  )
  if (has_partition_index_param) closure_params <- closure_params - 1
  closure_args <- c(
    list(df),
    if (!is.null(funcContext$user_context)) list(funcContext$user_context) else NULL,
    lapply(grouped_by, function(group_by_name) df[[group_by_name]][[1]]),
    barrier_arg
  )[0:closure_params]
  if (has_partition_index_param) {
    closure_args[[funcContext$partition_index_param]] <- partition_index
  }

  worker_log("computing closure")
  result <- do.call(closure, closure_args)
  worker_log("computed closure")

  as_factors <- getOption("stringsAsFactors")
  on.exit(options(stringsAsFactors = as_factors))
  options(stringsAsFactors = FALSE)

  if (identical(fetch_result_as_sdf, FALSE)) {
    result <- lapply(result, function(x) serialize(x, NULL))
    class(result) <- c("spark_apply_binary_result", class(result))
    result <- tibble::tibble(spark_apply_binary_result = result)
  }

  if (!"data.frame" %in% class(result)) {
    worker_log("data.frame expected but ", class(result), " found")

    result <- as.data.frame(result)
  }

  result
}

spark_worker_clean_factors <- function(result) {
  if (any(sapply(result, is.factor))) {
    result <- as.data.frame(lapply(result, function(x) if (is.factor(x)) as.character(x) else x), stringsAsFactors = FALSE)
  }

  result
}

spark_worker_maybe_serialize_list_cols_as_json <- function(config, result) {
  if (identical(config$fetch_result_as_sdf, TRUE) &&
    config$spark_version >= "2.4.0" &&
    any(sapply(result, is.list))) {
    result <- do.call(
      tibble::tibble,
      lapply(
        result,
        function(x) {
          if (is.list(x)) {
            x <- sapply(
              x,
              function(e) jsonlite::toJSON(e, auto_unbox = TRUE, digits = NA)
            )
            class(x) <- c(class(x), "list_col_as_json")
          }
          x
        }
      )
    )
  }

  result
}

spark_worker_apply_maybe_schema <- function(config, result) {
  if (identical(config$schema, TRUE)) {
    worker_log("updating schema")

    col_names <- colnames(result)
    types <- list()
    json_cols <- list()

    for (i in seq_along(result)) {
      if ("list_col_as_json" %in% class(result[[i]])) {
        json_cols <- append(json_cols, col_names[[i]])
        types <- append(types, "character")
      } else {
        types <- append(types, class(result[[i]])[[1]])
      }
    }

    result <- data.frame(
      names = paste(col_names, collapse = "|"),
      types = paste(types, collapse = "|"),
      json_cols = paste(json_cols, collapse = "|"),
      stringsAsFactors = FALSE
    )
  }

  result
}

spark_worker_build_types <- function(context, columns) {
  names <- names(columns)
  sqlutils <- worker_invoke(context, "getSqlUtils")
  fields <- worker_invoke(
    sqlutils,
    "createStructFields",
    lapply(
      names,
      function(name) {
        list(name, columns[[name]][[1]], TRUE)
      }
    )
  )

  worker_invoke(sqlutils, "createStructType", fields)
}

spark_worker_get_group_batch <- function(batch) {
  worker_invoke(
    batch, "get", 0L
  )
}

spark_worker_add_group_by_column <- function(df, result, grouped, grouped_by) {
  if (grouped) {
    if (nrow(result) > 0) {
      new_column_values <- lapply(grouped_by, function(grouped_by_name) df[[grouped_by_name]][[1]])
      names(new_column_values) <- grouped_by

      if ("AsIs" %in% class(result)) class(result) <- class(result)[-match("AsIs", class(result))]
      result <- do.call("cbind", list(new_column_values, result))

      names(result) <- gsub("\\.", "_", make.unique(names(result)))
    }
    else {
      result <- NULL
    }
  }

  result
}

get_arrow_converters <- function(context, config) {
  if (config$spark_version < "2.3.0") {
    stop("ArrowConverters is only supported for Spark 2.3 or above.")
  }

  worker_invoke(context, "getArrowConverters")
}

get_arrow_converters_impl <- function(context, config) {
  if (config$spark_version < "2.3.0") {
    stop("ArrowConverters is only supported for Spark 2.3 or above.")
  }

  worker_invoke(context, "getArrowConvertersImpl")
}

spark_worker_apply_arrow <- function(sc, config) {
  worker_log("using arrow serializer")

  context <- spark_worker_context(sc)
  spark_worker_init_packages(sc, context)

  closure <- unserialize(worker_invoke(context, "getClosure"))
  funcContext <- unserialize(worker_invoke(context, "getContext"))
  grouped_by <- worker_invoke(context, "getGroupBy")
  grouped <- !is.null(grouped_by) && length(grouped_by) > 0
  columnNames <- worker_invoke(context, "getColumns")
  schema_input <- worker_invoke(context, "getSchema")
  time_zone <- worker_invoke(context, "getTimeZoneId")
  options_map <- worker_invoke(context, "getOptions")
  barrier_map <- as.list(worker_invoke(context, "getBarrier"))
  partition_index <- worker_invoke(context, "getPartitionIndex")

  if (grouped) {
    record_batch_raw_groups <- worker_invoke(context, "getSourceArray")
    record_batch_raw_groups_idx <- 1
    record_batch_raw <- spark_worker_get_group_batch(record_batch_raw_groups[[record_batch_raw_groups_idx]])
  } else {
    row_iterator <- worker_invoke(context, "getIterator")
    arrow_converters_impl <- get_arrow_converters_impl(context, config)
    record_batch_raw <- worker_invoke(
      arrow_converters_impl,
      "toBatchArray",
      row_iterator,
      schema_input,
      time_zone,
      as.integer(options_map[["maxRecordsPerBatch"]])
    )
  }

  reader <- arrow_record_stream_reader(record_batch_raw)
  record_entry <- arrow_read_record_batch(reader)

  all_batches <- list()
  total_rows <- 0

  schema_output <- NULL

  batch_idx <- 0
  while (!is.null(record_entry)) {
    batch_idx <- batch_idx + 1
    worker_log("is processing batch ", batch_idx)

    df <- arrow_as_tibble(record_entry)
    result <- NULL

    if (!is.null(df)) {
      colnames(df) <- columnNames[seq_along(colnames(df))]

      result <- spark_worker_execute_closure(
        closure,
        df,
        funcContext,
        grouped_by,
        barrier_map,
        config$fetch_result_as_sdf,
        partition_index
      )

      result <- spark_worker_add_group_by_column(df, result, grouped, grouped_by)

      result <- spark_worker_clean_factors(result)

      result <- spark_worker_maybe_serialize_list_cols_as_json(config, result)

      result <- spark_worker_apply_maybe_schema(config, result)
    }

    if (!is.null(result)) {
      if (is.null(schema_output)) {
        schema_output <- spark_worker_build_types(context, lapply(result, class))
      }
      raw_batch <- arrow_write_record_batch(result, config$spark_version)

      all_batches[[length(all_batches) + 1]] <- raw_batch
      total_rows <- total_rows + nrow(result)
    }

    record_entry <- arrow_read_record_batch(reader)

    if (grouped && is.null(record_entry) && record_batch_raw_groups_idx < length(record_batch_raw_groups)) {
      record_batch_raw_groups_idx <- record_batch_raw_groups_idx + 1
      record_batch_raw <- spark_worker_get_group_batch(record_batch_raw_groups[[record_batch_raw_groups_idx]])

      reader <- arrow_record_stream_reader(record_batch_raw)
      record_entry <- arrow_read_record_batch(reader)
    }
  }

  if (length(all_batches) > 0) {
    worker_log("updating ", total_rows, " rows using ", length(all_batches), " row batches")

    arrow_converters <- get_arrow_converters(context, config)
    row_iter <- worker_invoke(arrow_converters, "fromPayloadArray", all_batches, schema_output)

    worker_invoke(context, "setResultIter", row_iter)
    worker_log("updated ", total_rows, " rows using ", length(all_batches), " row batches")
  } else {
    worker_log("found no rows in closure result")
  }

  worker_log("finished apply")
}

spark_worker_apply <- function(sc, config) {
  context <- spark_worker_context(sc)
  spark_worker_init_packages(sc, context)

  grouped_by <- worker_invoke(context, "getGroupBy")
  grouped <- !is.null(grouped_by) && length(grouped_by) > 0
  if (grouped) worker_log("working over grouped data")

  length <- worker_invoke(context, "getSourceArrayLength")
  worker_log("found ", length, " rows")

  groups <- worker_invoke(context, if (grouped) "getSourceArrayGroupedSeq" else "getSourceArraySeq")
  worker_log("retrieved ", length(groups), " rows")

  closureRaw <- worker_invoke(context, "getClosure")
  closure <- unserialize(closureRaw)

  funcContextRaw <- worker_invoke(context, "getContext")
  funcContext <- unserialize(funcContextRaw)

  closureRLangRaw <- worker_invoke(context, "getClosureRLang")
  if (length(closureRLangRaw) > 0) {
    worker_log("found rlang closure")
    closureRLang <- spark_worker_rlang_unserialize()
    if (!is.null(closureRLang)) {
      closure <- closureRLang(closureRLangRaw)
      worker_log("created rlang closure")
    }
  }

  if (identical(config$schema, TRUE)) {
    worker_log("is running to compute schema")
  }

  columnNames <- worker_invoke(context, "getColumns")
  barrier_map <- as.list(worker_invoke(context, "getBarrier"))
  partition_index <- worker_invoke(context, "getPartitionIndex")

  if (!grouped) groups <- list(list(groups))

  all_results <- NULL

  for (group_entry in groups) {
    # serialized groups are wrapped over single lists
    data <- group_entry[[1]]

    df <- (
      if (config$single_binary_column) {
        tibble::tibble(encoded = lapply(data, function(x) x[[1]]))
      } else {
        do.call(rbind.data.frame, c(data, list(stringsAsFactors = FALSE)))
      })

    if (!config$single_binary_column) {
      # rbind removes Date classes so we re-assign them here
      if (length(data) > 0 && ncol(df) > 0 && nrow(df) > 0) {
        if (any(sapply(data[[1]], function(e) class(e)[[1]]) %in% c("Date", "POSIXct"))) {
          first_row <- data[[1]]
          for (idx in seq_along(first_row)) {
            first_class <- class(first_row[[idx]])[[1]]
            if (identical(first_class, "Date")) {
              df[[idx]] <- as.Date(df[[idx]], origin = "1970-01-01")
            } else if (identical(first_class, "POSIXct")) {
              df[[idx]] <- as.POSIXct(df[[idx]], origin = "1970-01-01")
            }
          }
        }

        # cast column to correct type, for instance, when dealing with NAs.
        for (i in seq_along(df)) {
          target_type <- funcContext$column_types[[i]]
          if (!is.null(target_type) && class(df[[i]]) != target_type) {
            df[[i]] <- do.call(paste("as", target_type, sep = "."), args = list(df[[i]]))
          }
        }
      }
    }

    colnames(df) <- columnNames[seq_along(colnames(df))]

    result <- spark_worker_execute_closure(
      closure,
      df,
      funcContext,
      grouped_by,
      barrier_map,
      config$fetch_result_as_sdf,
      partition_index
    )

    result <- spark_worker_add_group_by_column(df, result, grouped, grouped_by)

    result <- spark_worker_clean_factors(result)

    result <- spark_worker_maybe_serialize_list_cols_as_json(config, result)

    result <- spark_worker_apply_maybe_schema(config, result)

    all_results <- rbind(all_results, result)
  }

  if (!is.null(all_results) && nrow(all_results) > 0) {
    worker_log("updating ", nrow(all_results), " rows")

    all_data <- lapply(seq_len(nrow(all_results)), function(i) as.list(all_results[i, ]))

    worker_invoke(context, "setResultArraySeq", all_data)
    worker_log("updated ", nrow(all_results), " rows")
  } else {
    worker_log("found no rows in closure result")
  }

  worker_log("finished apply")
}

spark_worker_rlang_unserialize <- function() {
  rlang_unserialize <- core_get_package_function("rlang", "bytes_unserialise")
  if (is.null(rlang_unserialize)) {
    core_get_package_function("rlanglabs", "bytes_unserialise")
  } else {
    rlang_unserialize
  }
}

spark_worker_unbundle_path <- function() {
  file.path("sparklyr-bundle")
}

#' Extracts a bundle of dependencies required by \code{spark_apply()}
#'
#' @param bundle_path Path to the bundle created using \code{spark_apply_bundle()}
#' @param base_path Base path to use while extracting bundles
#'
#' @keywords internal
#' @export
worker_spark_apply_unbundle <- function(bundle_path, base_path, bundle_name) {
  extractPath <- file.path(base_path, spark_worker_unbundle_path(), bundle_name)
  lockFile <- file.path(extractPath, "sparklyr.lock")

  if (!dir.exists(extractPath)) dir.create(extractPath, recursive = TRUE)

  if (length(dir(extractPath)) == 0) {
    worker_log("found that the unbundle path is empty, extracting:", extractPath)

    writeLines("", lockFile)
    system2("tar", c("-xf", bundle_path, "-C", extractPath))
    unlink(lockFile)
  }

  if (file.exists(lockFile)) {
    worker_log("found that lock file exists, waiting")
    while (file.exists(lockFile)) {
      Sys.sleep(1)
    }
    worker_log("completed lock file wait")
  }

  extractPath
}

# nocov end
# nocov start

spark_worker_connect <- function(
                                 sessionId,
                                 backendPort = 8880,
                                 config = list()) {
  gatewayPort <- spark_config_value(config, "sparklyr.worker.gateway.port", backendPort)

  gatewayAddress <- spark_config_value(config, "sparklyr.worker.gateway.address", "localhost")
  config <- list()

  worker_log("is connecting to backend using port ", gatewayPort)

  gatewayInfo <- spark_connect_gateway(gatewayAddress,
    gatewayPort,
    sessionId,
    config = config,
    isStarting = TRUE
  )

  worker_log("is connected to backend")
  worker_log("is connecting to backend session")

  tryCatch(
    {
      interval <- spark_config_value(config, "sparklyr.backend.interval", 1)

      backend <- socketConnection(
        host = "localhost",
        port = gatewayInfo$backendPort,
        server = FALSE,
        blocking = interval > 0,
        open = "wb",
        timeout = interval
      )

      class(backend) <- c(class(backend), "shell_backend")
    },
    error = function(err) {
      close(gatewayInfo$gateway)

      stop(
        "Failed to open connection to backend:", err$message
      )
    }
  )

  worker_log("is connected to backend session")

  sc <- structure(class = c("spark_worker_connection"), list(
    # spark_connection
    master = "",
    method = "shell",
    app_name = NULL,
    config = NULL,
    state = new.env(),
    # spark_shell_connection
    spark_home = NULL,
    backend = backend,
    gateway = gatewayInfo$gateway,
    output_file = NULL
  ))

  worker_log("created connection")

  sc
}

# nocov end
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
# nocov start

worker_invoke_method <- function(sc, static, object, method, ...) {
  core_invoke_method(sc, static, object, method, ...)
}

worker_invoke <- function(jobj, method, ...) {
  UseMethod("worker_invoke")
}

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
# nocov start

worker_log_env <- new.env()

worker_log_session <- function(sessionId) {
  assign("sessionId", sessionId, envir = worker_log_env)
}

worker_log_format <- function(message, session, level = "INFO", component = "RScript") {
  paste(
    format(Sys.time(), "%y/%m/%d %H:%M:%S"),
    " ",
    level,
    " sparklyr: ",
    component,
    " (",
    session,
    ") ",
    message,
    sep = ""
  )
}

worker_log_level <- function(..., level, component = "RScript") {
  if (is.null(worker_log_env$sessionId)) {
    worker_log_env <- get0("worker_log_env", envir = .GlobalEnv)
    if (is.null(worker_log_env$sessionId)) {
      return()
    }
  }

  args <- list(...)
  message <- paste(args, sep = "", collapse = "")
  formatted <- worker_log_format(message, worker_log_env$sessionId,
    level = level, component = component
  )
  cat(formatted, "\n")
}

worker_log <- function(...) {
  worker_log_level(..., level = "INFO")
}

worker_log_warning <- function(...) {
  worker_log_level(..., level = "WARN")
}

worker_log_error <- function(...) {
  worker_log_level(..., level = "ERROR")
}

# nocov end
# nocov start

.worker_globals <- new.env(parent = emptyenv())

spark_worker_main <- function(
                              sessionId,
                              backendPort = 8880,
                              configRaw = NULL) {
  spark_worker_hooks()

  tryCatch(
    {
      worker_log_session(sessionId)

      if (is.null(configRaw)) configRaw <- worker_config_serialize(list())

      config <- worker_config_deserialize(configRaw)

      if (identical(config$profile, TRUE)) {
        profile_name <- paste("spark-apply-", as.numeric(Sys.time()), ".Rprof", sep = "")
        worker_log("starting new profile in ", file.path(getwd(), profile_name))
        utils::Rprof(profile_name)
      }

      if (config$debug) {
        worker_log("exiting to wait for debugging session to attach")

        # sleep for 1 day to allow long debugging sessions
        Sys.sleep(60 * 60 * 24)
        return()
      }

      worker_log("is starting")

      options(sparklyr.connection.cancellable = FALSE)

      sc <- spark_worker_connect(sessionId, backendPort, config)
      worker_log("is connected")

      if (config$arrow) {
        spark_worker_apply_arrow(sc, config)
      }
      else {
        spark_worker_apply(sc, config)
      }

      if (identical(config$profile, TRUE)) {
        # utils::Rprof(NULL)
        worker_log("closing profile")
      }
    },
    error = function(e) {
      worker_log_error("terminated unexpectedly: ", e$message)
      if (exists(".stopLastError", envir = .worker_globals)) {
        worker_log_error("collected callstack: \n", get(".stopLastError", envir = .worker_globals))
      }
      quit(status = -1)
    }
  )

  worker_log("finished")
}

spark_worker_hooks <- function() {
  unlock <- get("unlockBinding")
  lock <- get("lockBinding")

  originalStop <- stop
  unlock("stop", as.environment("package:base"))
  assign("stop", function(...) {
    frame_names <- list()
    frame_start <- max(1, sys.nframe() - 5)
    for (i in frame_start:sys.nframe()) {
      current_call <- sys.call(i)
      frame_names[[1 + i - frame_start]] <- paste(i, ": ", paste(head(deparse(current_call), 5), collapse = "\n"), sep = "")
    }

    assign(".stopLastError", paste(rev(frame_names), collapse = "\n"), envir = .worker_globals)
    originalStop(...)
  }, as.environment("package:base"))
  lock("stop", as.environment("package:base"))
}

# nocov end
do.call(spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))
