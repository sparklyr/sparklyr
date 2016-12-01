shell_connection_validate_config <- function(config) {
  if ("spark.jars.default" %in% names(config)) {
    warning("The spark.jars.default config parameter is deprecated, please use sparklyr.jars.default")
    config[["sparklyr.jars.default"]] <- config[["spark.jars.default"]]
  }

  config
}

# create a shell connection
shell_connection <- function(master,
                             spark_home,
                             app_name,
                             version,
                             hadoop_version,
                             shell_args,
                             config,
                             service,
                             extensions) {
  # trigger deprecated warnings
  config <- shell_connection_validate_config(config)

  # for local mode we support SPARK_HOME via locally installed versions
  if (spark_master_is_local(master)) {
    if (!nzchar(spark_home)) {
      installInfo <- spark_install_find(version, hadoop_version, latest = FALSE, connecting = TRUE)
      spark_home <- installInfo$sparkVersionDir
    }
  }

  # prepare windows environment
  prepare_windows_environment(spark_home)

  # verify that java is available
  validate_java_version(spark_home)

  # error if there is no SPARK_HOME
  if (!nzchar(spark_home))
    stop("Failed to connect to Spark (SPARK_HOME is not set).")

  # apply environment variables
  environment <- list()
  sparkEnvironmentVars <- connection_config(list(config = config, master = master), "spark.env.")
  lapply(names(sparkEnvironmentVars), function(varName) {
    environment[[varName]] <<- sparkEnvironmentVars[[varName]]
  })

  # start shell
  start_shell(
    master = master,
    spark_home = spark_home,
    spark_version = version,
    app_name = app_name,
    config = config,
    jars = spark_config_value(config, "sparklyr.jars.default", list()),
    packages = spark_config_value(config, "sparklyr.defaultPackages"),
    extensions = extensions,
    environment = environment,
    shell_args = shell_args,
    service = service
  )
}

spark_session_id <- function(app_name, master) {
  hex_to_int <- function(h) {
    xx = strsplit(tolower(h), "")[[1L]]
    pos = match(xx, c(0L:9L, letters[1L:6L]))
    sum((pos - 1L) * 16^(rev(seq_along(xx) - 1)))
  }

  hashed <- digest(object = paste(app_name, master, sep = ""), algo = "crc32")
  hex_to_int(hashed) %% .Machine$integer.max
}

abort_shell <- function(message, spark_submit_path, shell_args, output_file, error_file) {
  withr::with_options(list(
    warning.length = 8000
  ), {
    maxRows <- 50

    logLines <- if (!is.null(output_file) && file.exists(output_file))
      paste(tail(readLines(output_file), n = maxRows), collapse = "\n")
    else ""

    errorLines <- if (!is.null(error_file) && file.exists(error_file))
      paste(tail(readLines(error_file), n = maxRows), collapse = "\n")
    else ""

    stop(
      paste(
        message, "\n",
        if (!is.null(spark_submit_path))
          paste("    Path: ", spark_submit_path, "\n", sep = "") else "",
        if (!is.null(shell_args))
          paste("    Parameters: ", paste(shell_args, collapse = ", "), "\n", sep = "") else  "",
        "\n\n",
        if (!is.null(output_file)) "---- Output Log ----\n" else "",
        logLines,
        "\n\n",
        if (!is.null(error_file)) "---- Error Log ----\n" else "",
        errorLines,
        sep = ""
      )
    )
  })
}

# Start the Spark R Shell
start_shell <- function(master,
                        spark_home = Sys.getenv("SPARK_HOME"),
                        spark_version = NULL,
                        app_name = "sparklyr",
                        config = list(),
                        extensions = sparklyr::registered_extensions(),
                        jars = NULL,
                        packages = NULL,
                        environment = NULL,
                        shell_args = NULL,
                        service = FALSE) {

  gatewayPort <- as.integer(spark_config_value(config, "sparklyr.gateway.port", "8880"))
  gatewayAddress <- spark_config_value(config, "sparklyr.gateway.address", "localhost")
  isService <- FALSE

  sessionId <- if (isService)
      spark_session_id(app_name, master)
  else
    floor(stats::runif(1, min = 0, max = 10000))

  # attempt to connect into an existing gateway
  gatewayInfo <- spark_connect_gateway(gatewayAddress = gatewayAddress,
                                       gatewayPort = gatewayPort,
                                       sessionId = sessionId,
                                       config = config)

  output_file <- NULL
  error_file <- NULL
  spark_submit_path <- NULL

  shQuoteType <- if (.Platform$OS.type == "windows") "cmd" else NULL

  if (is.null(gatewayInfo) || gatewayInfo$backendPort == 0)
  {
    # read app jar through config, this allows "sparkr-shell" to test sparkr backend
    app_jar <- spark_config_value(config, "sparklyr.app.jar", NULL)
    if (is.null(app_jar)) {
      versionSparkHome <- spark_version_from_home(spark_home, default = spark_version)

      app_jar <- spark_default_app_jar(versionSparkHome)
      if (typeof(app_jar) != "character" || nchar(app_jar) == 0) {
        stop("sparklyr does not currently support Spark version: ", versionSparkHome)
      }

      if (compareVersion(versionSparkHome, "1.6") < 0) {
        warning("sparklyr does not currently support Spark version: ", versionSparkHome)
      }

      app_jar <- shQuote(normalizePath(app_jar, mustWork = FALSE), type = shQuoteType)
      shell_args <- c(shell_args, "--class", "sparklyr.Backend")
    }

    # validate and normalize spark_home
    if (!nzchar(spark_home))
      stop("No spark_home specified (defaults to SPARK_HOME environment varirable).")
    if (!dir.exists(spark_home))
      stop("SPARK_HOME directory '", spark_home ,"' not found")
    spark_home <- normalizePath(spark_home)

    # set SPARK_HOME into child process environment
    if (is.null(environment()))
      environment <- list()
    environment$SPARK_HOME <- spark_home

    # provide empty config if necessary
    if (is.null(config))
      config <- list()

    # determine path to spark_submit
    spark_submit <- switch(.Platform$OS.type,
                           unix = "spark-submit",
                           windows = "spark-submit2.cmd"
    )
    spark_submit_path <- normalizePath(file.path(spark_home, "bin", spark_submit))

    # resolve extensions
    spark_version <- numeric_version(
      ifelse(is.null(spark_version),
             spark_version_from_home(spark_home),
             gsub("[-_a-zA-Z]", "", spark_version)
      )
    )
    if (spark_version < "2.0")
      scala_version <- numeric_version("2.10")
    else
      scala_version <- numeric_version("2.11")
    extensions <- spark_dependencies_from_extensions(spark_version, scala_version, extensions)

    # combine passed jars and packages with extensions
    all_jars <- c(jars, extensions$jars)
    jars <- if (length(all_jars) > 0) normalizePath(unlist(unique(all_jars))) else list()
    packages <- unique(c(packages, extensions$packages))

    # include embedded jars, if needed
    if (!is.null(config[["sparklyr.csv.embedded"]]) &&
        length(grep(config[["sparklyr.csv.embedded"]], spark_version)) > 0) {
      jars <- c(
        jars,
        system.file(file.path("java", "spark-csv_2.11-1.3.0.jar"), package = "sparklyr"),
        system.file(file.path("java", "commons-csv-1.1.jar"), package = "sparklyr"),
        system.file(file.path("java", "univocity-parsers-1.5.1.jar"), package = "sparklyr")
      )
    }

    # add jars to arguments
    if (length(jars) > 0) {
      shell_args <- c(shell_args, "--jars", paste(shQuote(jars, type = shQuoteType), collapse=","))
    }

    # add packages to arguments
    if (length(packages) > 0) {
      shell_args <- c(shell_args, "--packages", paste(shQuote(packages, type = shQuoteType), collapse=","))
    }

    # add environment parameters to arguments
    shell_env_args <- Sys.getenv("sparklyr.shell.args")
    if (nchar(shell_env_args) > 0) {
      shell_args <- c(shell_args, strsplit(shell_env_args, " ")[[1]])
    }

    # add app_jar to args
    shell_args <- c(shell_args, app_jar)

    # prepare spark-submit shell arguments
    shell_args <- c(shell_args, as.character(gatewayPort), sessionId)

    if (isService) {
      shell_args <- c(shell_args, "--service")
    }

    isRemote <- FALSE
    if (isService && isRemote) {
      shell_args <- c(shell_args, "--remote")
    }

    # create temp file for stdout and stderr
    output_file <- tempfile(fileext = "_spark.log")
    error_file <- tempfile(fileext = "_spark.err")

    # start the shell (w/ specified additional environment variables)
    env <- unlist(environment)
    withr::with_envvar(env, {
      system2(spark_submit_path,
        args = shell_args,
        stdout = output_file,
        stderr = output_file,
        wait = FALSE)
    })

    tryCatch({
      # connect and wait for the service to start
      gatewayInfo <- spark_connect_gateway(gatewayAddress,
                                           gatewayPort,
                                           sessionId,
                                           config = config,
                                           isStarting = TRUE)
    }, error = function(e) {
      abort_shell(
        paste(
          "Failed while connecting to sparklyr to port (",
          gatewayPort,
          ") for sessionid (",
          sessionId,
          "): ",
          e$message,
          sep = ""
        ),
        spark_submit_path,
        shell_args,
        output_file,
        error_file
      )
    })
  }

  tryCatch({
    # set timeout for socket connection
    timeout <- spark_config_value(config, "sparklyr.backend.timeout", 30 * 24 * 60 * 60)
    backend <- socketConnection(host = "localhost",
                                port = gatewayInfo$backendPort,
                                server = FALSE,
                                blocking = TRUE,
                                open = "wb",
                                timeout = timeout)
  }, error = function(err) {
    close(gatewayInfo$gateway)

    abort_shell(
      paste("Failed to open connection to backend:", err$message),
      spark_submit_path,
      shell_args,
      output_file,
      error_file
    )
  })

  # create the shell connection
  sc <- structure(class = c("spark_connection", "spark_shell_connection"), list(
    # spark_connection
    master = master,
    method = "shell",
    app_name = app_name,
    config = config,
    # spark_shell_connection
    spark_home = spark_home,
    backend = backend,
    monitor = gatewayInfo$gateway,
    output_file = output_file
  ))

  # stop shell on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc)
    }
  }, onexit = TRUE)

  sc
}

#' @export
spark_disconnect.spark_shell_connection <- function(sc, ...) {
  stop_shell(sc, ...)
}

# Stop the Spark R Shell
stop_shell <- function(sc, terminate = FALSE) {
  terminationMode <- if (terminate == TRUE) "terminateBackend" else "stopBackend"
  invoke_method(sc,
                FALSE,
                "Handler",
                terminationMode)

  close(sc$backend)
  close(sc$monitor)

  # allow time to clean resources for multiple spark_connect/spark_disconnect calls
  Sys.sleep(1)
}

#' @export
connection_is_open.spark_shell_connection <- function(sc) {
  bothOpen <- FALSE
  if (!identical(sc, NULL)) {
    tryCatch({
      bothOpen <- isOpen(sc$backend) && isOpen(sc$monitor)
    }, error = function(e) {
    })
  }
  bothOpen
}

#' @export
spark_log.spark_shell_connection <- function(sc, n = 100, filter = NULL, ...) {
  if (!is.null(sc$output_file) && file.exists(sc$output_file)) {
    log <- file(sc$output_file)
    lines <- readLines(log)
    close(log)

    if (!is.null(filter)) {
      lines <- lines[grepl(filter, lines)]
    }

    if (!is.null(n))
      linesLog <- utils::tail(lines, n = n)
    else
      linesLog <- lines
  }
  else {
    linesLog <- "spark log is not available"
  }

  attr(linesLog, "class") <- "spark_log"
  linesLog
}

#' @export
spark_web.spark_shell_connection <- function(sc, ...) {
  lines <- spark_log(sc, n = 200)

  uiLine <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine)
    match <-regmatches(uiLine, matches)
    if (length(match) > 0) {
      return(structure(match, class = "spark_web_url"))
    }
  }

  uiLine <- grep(".*Bound SparkUI to.*", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexec(".*Bound SparkUI to.*and started at (http.*)", uiLine)
    match <- regmatches(uiLine, matches)
    if (length(match) > 0 && length(match[[1]]) > 1) {
      return(structure(match[[1]][[2]], class = "spark_web_url"))
    }
  }

  warning("Spark UI URL not found in logs, attempting to guess.")
  structure("http://localhost:4040", class = "spark_web_url")
}

#' @export
invoke_method.spark_shell_connection <- function(sc, static, object, method, ...)
{
  if (is.null(sc)) {
    stop("The connection is no longer valid.")
  }

  # if the object is a jobj then get it's id
  if (inherits(object, "spark_jobj"))
    object <- object$id

  rc <- rawConnection(raw(), "r+")
  writeString(rc, object)
  writeBoolean(rc, static)
  writeString(rc, method)

  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)

  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)

  backend <- sc$backend
  writeBin(con, backend)

  returnStatus <- readInt(backend)
  if (length(returnStatus) == 0)
    stop("No status is returned. Spark R backend might have failed.")
  if (returnStatus != 0) {
    # get error message from backend and report to R
    msg <- readString(backend)
    withr::with_options(list(
      warning.length = 8000
    ), {
      if (nzchar(msg))
        stop(msg, call. = FALSE)
      else {
        # read the spark log
        msg <- read_spark_log_error(sc)
        stop(msg, call. = FALSE)
      }
    })
  }

  class(backend) <- c(class(backend), "shell_backend")

  object <- readObject(backend)
  attach_connection(object, sc)
}

jobj_subclass.shell_backend <- function(con) {
  "shell_jobj"
}

#' @export
print_jobj.spark_shell_connection <- function(sc, jobj, ...) {
  if (connection_is_open(sc)) {
    info <- jobj_info(jobj)
    fmt <- "<jobj[%s]>\n  %s\n  %s\n"
    cat(sprintf(fmt, jobj$id, info$class, info$repr))
  } else {
    fmt <- "<jobj[%s]>\n  <detached>"
    cat(sprintf(fmt, jobj$id))
  }
}

read_spark_log_error <- function(sc) {
  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- readr::read_lines(sc$output_file)
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

spark_config_value <- function(config, name, default = NULL) {
  if (is.null(config[[name]])) default else config[[name]]
}

#' @export
initialize_connection.spark_shell_connection <- function(sc) {
  # initialize and return the connection
  tryCatch({
    sc$spark_context <- invoke_static(sc, "sparklyr.Backend", "getSparkContext")

    if (is.null(sc$spark_context)) {
      # create the spark config
      conf <- invoke_new(sc, "org.apache.spark.SparkConf")
      conf <- invoke(conf, "setAppName", sc$app_name)
      conf <- invoke(conf, "setMaster", sc$master)
      conf <- invoke(conf, "setSparkHome", sc$spark_home)

      context_config <- connection_config(sc, "spark.", c("spark.sql."))
      apply_config(context_config, conf, "set", "spark.")

      # create the spark context and assign the connection to it
      sc$spark_context <- invoke_static(
        sc,
        "org.apache.spark.SparkContext",
        "getOrCreate",
        conf
      )

      invoke_static(sc, "sparklyr.Backend", "setSparkContext", sc$spark_context)
    }

    sc$spark_context$connection <- sc

    # create the java spark context and assign the connection to it
    sc$java_context <- invoke_static(
      sc,
      "org.apache.spark.api.java.JavaSparkContext",
      "fromSparkContext",
      sc$spark_context
    )
    sc$java_context$connection <- sc

    # create the hive context and assign the connection to it
    sc$hive_context <- create_hive_context(sc)
    sc$hive_context$connection <- sc

    # return the modified connection
    sc
  }, error = function(e) {
    abort_shell(
      paste("Failed during initialize_connection:", e$message),
      spark_submit_path = NULL,
      shell_args = NULL,
      output_file = sc$output_file,
      error_file = NULL
    )
  })
}

#' @export
invoke.shell_jobj <- function(jobj, method, ...) {
  invoke_method(spark_connection(jobj), FALSE, jobj, method, ...)
}

#' @export
invoke_static.spark_shell_connection <- function(sc, class, method, ...) {
  invoke_method(sc, TRUE, class, method, ...)
}

#' @export
invoke_new.spark_shell_connection <- function(sc, class, ...) {
  invoke_method(sc, TRUE, class, "<init>", ...)
}

