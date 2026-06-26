# nocov start

.worker_globals <- new.env(parent = emptyenv())

spark_worker_main <- function(
  sessionId,
  backendPort = 8880,
  configRaw = NULL
) {
  spark_worker_hooks()
  tryCatch(
    {
      worker_log_session(sessionId)

      if (is.null(configRaw)) {
        configRaw <- worker_config_serialize(list())
      }

      config <- worker_config_deserialize(configRaw)

      if (identical(config$profile, TRUE)) {
        profile_name <- paste(
          "spark-apply-",
          as.numeric(Sys.time()),
          ".Rprof",
          sep = ""
        )
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
      } else {
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
        worker_log_error(
          "collected callstack: \n",
          get(".stopLastError", envir = .worker_globals)
        )
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
  assign(
    "stop",
    function(...) {
      frame_names <- list()
      frame_start <- max(1, sys.nframe() - 5)
      for (i in frame_start:sys.nframe()) {
        current_call <- sys.call(i)
        frame_names[[1 + i - frame_start]] <- paste(
          i,
          ": ",
          paste(head(deparse(current_call), 5), collapse = "\n"),
          sep = ""
        )
      }

      assign(
        ".stopLastError",
        paste(rev(frame_names), collapse = "\n"),
        envir = .worker_globals
      )
      originalStop(...)
    },
    as.environment("package:base")
  )
  lock("stop", as.environment("package:base"))
}

# nocov end

# nocov start

worker_log_env <- new.env()

worker_log_session <- function(sessionId) {
  assign("sessionId", sessionId, envir = worker_log_env)
}

worker_log_format <- function(
  message,
  session,
  level = "INFO",
  component = "RScript"
) {
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
  formatted <- worker_log_format(
    message,
    worker_log_env$sessionId,
    level = level,
    component = component
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
#' @export
connection_is_open.spark_worker_connection <- function(sc) {
  bothOpen <- FALSE
  if (!identical(sc, NULL)) {
    tryCatch(
      {
        bothOpen <- isOpen(sc$backend) && isOpen(sc$gateway)
      },
      error = function(e) {}
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

spark_worker_connect <- function(
  sessionId,
  backendPort = 8880,
  config = list()
) {
  gatewayPort <- spark_config_value(
    config,
    "sparklyr.worker.gateway.port",
    backendPort
  )

  gatewayAddress <- spark_config_value(
    config,
    "sparklyr.worker.gateway.address",
    "localhost"
  )
  config <- list()

  worker_log("is connecting to backend using port ", gatewayPort)

  gatewayInfo <- spark_connect_gateway(
    gatewayAddress,
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
        "Failed to open connection to backend:",
        err$message
      )
    }
  )

  worker_log("is connected to backend session")

  sc <- structure(
    class = c("spark_worker_connection"),
    list(
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
    )
  )

  worker_log("created connection")

  sc
}

# nocov end

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
