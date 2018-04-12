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
                             remote,
                             extensions) {
  # trigger deprecated warnings
  config <- shell_connection_validate_config(config)

  # for local mode we support SPARK_HOME via locally installed versions and version overrides SPARK_HOME
  if (spark_master_is_local(master)) {
    if (!nzchar(spark_home) || !is.null(version)) {
      installInfo <- spark_install_find(version, hadoop_version, latest = FALSE, hint = TRUE)
      spark_home <- installInfo$sparkVersionDir

      if (is.null(version))
        message("* Using Spark: ", installInfo$sparkVersion)
    }
  }

  # for yarn-cluster set deploy mode as shell arguments
  if (spark_master_is_yarn_cluster(master, config)) {
    if (is.null(config[["sparklyr.shell.deploy-mode"]])) {
      shell_args <- c(shell_args, "--deploy-mode", "cluster")
    }

    if (is.null(config[["sparklyr.shell.master"]])) {
      shell_args <- c(shell_args, "--master", "yarn")
    }
  }

  # start with blank environment variables
  environment <- new.env()

  # prepare windows environment
  prepare_windows_environment(spark_home, environment)

  # verify that java is available
  validate_java_version(master, spark_home)

  # error if there is no SPARK_HOME
  if (!nzchar(spark_home))
    stop("Failed to connect to Spark (SPARK_HOME is not set).")

  # apply environment variables
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
    service = service,
    remote = remote
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

spark_session_random <- function() {
  floor(openssl::rand_num(1) * 100000)
}

abort_shell <- function(message, spark_submit_path, shell_args, output_file, error_file) {
  withr::with_options(list(
    warning.length = 8000
  ), {
    maxRows <- 100

    # sleep for one second to allow spark process to dump data into the log file
    Sys.sleep(1)

    logLines <- if (!is.null(output_file) && file.exists(output_file)) {
      outputContent <- readLines(output_file)
      paste(tail(outputContent, n = maxRows), collapse = "\n")
    } else ""

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
        if (!is.null(output_file))
          paste("    Log: ", output_file, "\n", sep = "") else "",
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
                        service = FALSE,
                        remote = FALSE) {

  gatewayPort <- as.integer(spark_config_value(config, "sparklyr.gateway.port", "8880"))
  gatewayAddress <- spark_config_value(config, "sparklyr.gateway.address", "localhost")
  isService <- service
  isRemote <- remote

  sessionId <- if (isService)
      spark_session_id(app_name, master)
  else
      spark_session_random()

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
        stop("sparklyr does not support Spark version: ", versionSparkHome)
      }

      if (compareVersion(versionSparkHome, "1.6") < 0) {
        warning("sparklyr does not support Spark version: ", versionSparkHome)
      }

      app_jar <- shQuote(normalizePath(app_jar, mustWork = FALSE), type = shQuoteType)
      shell_args <- c(shell_args, "--class", "sparklyr.Shell")
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
                           unix = c("spark2-submit", "spark-submit"),
                           windows = "spark-submit2.cmd")

    spark_submit_paths <- unlist(lapply(
      spark_submit,
      function(submit_path) {
        normalizePath(file.path(spark_home, "bin", submit_path), mustWork = FALSE)
      }))

    if (!any(file.exists(spark_submit_paths))) {
      stop("Failed to find spark-submit under '", spark_home, "', please verify SPARK_HOME.")
    }

    spark_submit_path <- spark_submit_paths[[which(file.exists(spark_submit_paths))[[1]]]]

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
        normalizePath(system.file(file.path("java", "spark-csv_2.11-1.5.0.jar"), package = "sparklyr")),
        normalizePath(system.file(file.path("java", "commons-csv-1.5.jar"), package = "sparklyr")),
        normalizePath(system.file(file.path("java", "univocity-parsers-1.5.1.jar"), package = "sparklyr"))
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

    # add custom backend args
    shell_args <- c(shell_args, paste(config[["sparklyr.backend.args"]]))

    if (isService) {
      shell_args <- c(shell_args, "--service")
    }

    if (isRemote) {
      shell_args <- c(shell_args, "--remote")
    }

    # create temp file for stdout and stderr
    output_file <- Sys.getenv("SPARKLYR_LOG_FILE", tempfile(fileext = "_spark.log"))
    error_file <- Sys.getenv("SPARKLYR_LOG_FILE", tempfile(fileext = "_spark.err"))

    console_log <- spark_config_exists(config, "sparklyr.log.console", FALSE)

    stdout_param <- if (console_log) "" else output_file
    stderr_param <- if (console_log) "" else output_file

    start_time <- floor(as.numeric(Sys.time()) * 1000)

    # start the shell (w/ specified additional environment variables)
    env <- unlist(as.list(environment))
    withr::with_envvar(env, {
      system2(spark_submit_path,
        args = shell_args,
        stdout = stdout_param,
        stderr = stderr_param,
        wait = FALSE)
    })

    # for yarn-cluster
    if (spark_master_is_yarn_cluster(master, config) && is.null(config[["sparklyr.gateway.address"]])) {
      gatewayAddress <- config[["sparklyr.gateway.address"]] <- spark_yarn_cluster_get_gateway(config, start_time)
    }

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
          if (spark_master_is_yarn_cluster(master, config)) {
            paste0(
              ") and address (",
              config[["sparklyr.gateway.address"]]
            )
          }
          else {
            ""
          },
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
    backend <- socketConnection(host = gatewayAddress,
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

  uiLine <- grep("Started SparkUI at ", lines, perl = TRUE, value = TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine)
    match <- regmatches(uiLine, matches)
    if (length(match) > 0) {
      return(structure(match, class = "spark_web_url"))
    }
  }

  uiLine <- grep(".*Bound SparkUI to.*", lines, perl = TRUE, value = TRUE)
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
  core_invoke_method(sc, static, object, method, ...)
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

shell_connection_config_defaults <- function() {
  list(
    spark.port.maxRetries = 128
  )
}

#' @export
initialize_connection.spark_shell_connection <- function(sc) {
  # initialize and return the connection
  tryCatch({
    backend <- invoke_static(sc, "sparklyr.Shell", "getBackend")
    sc$spark_context <- invoke(backend, "getSparkContext")

    if (is.null(sc$spark_context)) {
      # create the spark config
      conf <- invoke_new(sc, "org.apache.spark.SparkConf")
      conf <- invoke(conf, "setAppName", sc$app_name)

      if (!spark_master_is_yarn_cluster(sc$master, sc$config) &&
          !spark_master_is_gateway(sc$master)) {
        conf <- invoke(conf, "setMaster", sc$master)

        if (!is.null(sc$spark_home))
          conf <- invoke(conf, "setSparkHome", sc$spark_home)
      }

      context_config <- connection_config(sc, "spark.", c("spark.sql."))
      apply_config(context_config, conf, "set", "spark.")

      default_config <- shell_connection_config_defaults()
      default_config_remove <- Filter(function(e) e %in% names(context_config), names(default_config))
      default_config[default_config_remove] <- NULL
      apply_config(default_config, conf, "set", "spark.")

      # create the spark context and assign the connection to it
      sc$spark_context <- invoke_static(
        sc,
        "org.apache.spark.SparkContext",
        "getOrCreate",
        conf
      )

      invoke(backend, "setSparkContext", sc$spark_context)
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

