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
                             method = "",
                             app_name,
                             version,
                             hadoop_version,
                             shell_args,
                             config,
                             service,
                             remote,
                             extensions,
                             batch,
                             scala_version = NULL) {
  # trigger deprecated warnings
  config <- shell_connection_validate_config(config)

  if (spark_master_is_local(master)) {
    if (!nzchar(spark_home) || !is.null(version)) {
      installInfo <- spark_install_find(version, hadoop_version, latest = FALSE)
      if (method != "databricks-connect") {
        # Databricks Connect relies on set environment value of SPARK_HOME
        spark_home <- installInfo$sparkVersionDir
      }

      if (is.null(version) && interactive()) {
        message("* Using Spark: ", installInfo$sparkVersion)
      }
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
  if (spark_master_is_local(master)) {
    prepare_windows_environment(spark_home, environment)
  }

  # verify that java is available
  validate_java_version(master, spark_home)

  # error if there is no SPARK_HOME
  if (!nzchar(spark_home)) {
    stop("Failed to connect to Spark (SPARK_HOME is not set).")
  }

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
    jars = spark_config_value(config, c("sparklyr.connect.jars", "sparklyr.jars.default"), list()),
    packages = spark_config_value(config, c("sparklyr.connect.packages", "sparklyr.defaultPackages")),
    extensions = extensions,
    environment = environment,
    shell_args = shell_args,
    service = service,
    remote = remote,
    batch = batch,
    scala_version = scala_version
  )
}

spark_session_id <- function(app_name, master) {
  hex_to_int <- function(h) {
    xx <- strsplit(tolower(h), "")[[1L]]
    pos <- match(xx, c(0L:9L, letters[1L:6L]))
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
    } else {
      ""
    }

    errorLines <- if (!is.null(error_file) && file.exists(error_file)) {
      paste(tail(readLines(error_file), n = maxRows), collapse = "\n")
    } else {
      ""
    }

    stop(
      paste(
        message, "\n",
        if (!is.null(spark_submit_path)) {
          paste("    Path: ", spark_submit_path, "\n", sep = "")
        } else {
          ""
        },
        if (!is.null(shell_args)) {
          paste("    Parameters: ", paste(shell_args, collapse = ", "), "\n", sep = "")
        } else {
          ""
        },
        if (!is.null(output_file)) {
          paste("    Log: ", output_file, "\n", sep = "")
        } else {
          ""
        },
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

  print("\nLocal stack trace:\n")
  print(sys.calls())
}

# Start the Spark R Shell
start_shell <- function(master,
                        spark_home = Sys.getenv("SPARK_HOME"),
                        spark_version = NULL,
                        app_name = "sparklyr",
                        config = list(),
                        extensions,
                        jars = NULL,
                        packages = NULL,
                        environment = NULL,
                        shell_args = NULL,
                        service = FALSE,
                        remote = FALSE,
                        batch = NULL,
                        scala_version = NULL,
                        gateway_connect_attempts = 5,
                        gateway_connect_retry_interval_s = 0.25,
                        gateway_connect_retry_interval_multiplier = 2) {
  gatewayPort <- as.integer(spark_config_value(config, "sparklyr.gateway.port", "8880"))
  gatewayAddress <- spark_config_value(config, "sparklyr.gateway.address", "localhost")
  isService <- service
  isRemote <- remote
  isBatch <- !is.null(batch)

  sessionId <- if (isService) {
    spark_session_id(app_name, master)
  } else {
    spark_session_random()
  }

  # attempt to connect into an existing gateway
  gatewayInfo <- NULL
  if (spark_config_value(config, "sparklyr.gateway.routing", TRUE)) {
    gatewayInfo <- spark_connect_gateway(
      gatewayAddress = gatewayAddress,
      gatewayPort = gatewayPort,
      sessionId = sessionId,
      config = config
    )
  }

  output_file <- NULL
  error_file <- NULL
  spark_submit_path <- NULL

  shQuoteType <- if (.Platform$OS.type == "windows") "cmd" else NULL

  if (is.null(gatewayInfo) || gatewayInfo$backendPort == 0) {
    versionSparkHome <- spark_version_from_home(spark_home, default = spark_version)

    # read app jar through config, this allows "sparkr-shell" to test sparkr backend
    app_jar <- spark_config_value(config, c("sparklyr.connect.app.jar", "sparklyr.app.jar"), NULL)
    if (is.null(app_jar)) {
      app_jar <- spark_default_app_jar(versionSparkHome, scala_version = scala_version)
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
    if (!nzchar(spark_home)) {
      stop("No spark_home specified (defaults to SPARK_HOME environment varirable).")
    }
    if (!dir.exists(spark_home)) {
      stop("SPARK_HOME directory '", spark_home, "' not found")
    }
    spark_home <- normalizePath(spark_home)

    # set SPARK_HOME into child process environment
    if (is.null(environment())) {
      environment <- list()
    }
    environment$SPARK_HOME <- spark_home

    # provide empty config if necessary
    if (is.null(config)) {
      config <- list()
    }

    # determine path to spark_submit
    spark_submit <- switch(.Platform$OS.type,
      unix = c("spark2-submit", "spark-submit"),
      windows = "spark-submit2.cmd"
    )

    # allow users to override spark-submit if needed
    spark_submit <- spark_config_value(config, c("sparklyr.connect.sparksubmit", "sparklyr.spark-submit"), spark_submit)

    spark_submit_paths <- unlist(lapply(
      spark_submit,
      function(submit_path) {
        normalizePath(file.path(spark_home, "bin", submit_path), mustWork = FALSE)
      }
    ))

    if (!any(file.exists(spark_submit_paths))) {
      stop("Failed to find '", paste(spark_submit, collapse = "' or '"), "' under '", spark_home, "', please verify SPARK_HOME.")
    }

    spark_submit_path <- spark_submit_paths[[which(file.exists(spark_submit_paths))[[1]]]]

    # resolve extensions
    spark_version <- numeric_version(
      ifelse(is.null(spark_version),
        spark_version_from_home(spark_home),
        gsub("[-_a-zA-Z]", "", spark_version)
      )
    )
    extensions <- spark_dependencies_from_extensions(spark_version, scala_version, extensions, config)

    # combine passed jars and packages with extensions
    all_jars <- c(jars, extensions$jars)
    jars <- if (length(all_jars) > 0) normalizePath(unlist(unique(all_jars))) else list()
    packages <- unique(c(packages, extensions$packages))
    repositories <- extensions$repositories

    # include embedded jars, if needed
    csv_config_value <- spark_config_value(config, c("sparklyr.connect.csv.embedded", "sparklyr.csv.embedded"))
    if (!is.null(csv_config_value) &&
      length(grep(csv_config_value, spark_version)) > 0) {
      packages <- c(packages, "com.univocity:univocity-parsers:1.5.1")
      if (spark_config_value(config, c("sparklyr.connect.csv.scala11"), FALSE)) {
        packages <- c(
          packages,
          "com.databricks:spark-csv_2.11:1.5.0",
          "org.apache.commons:commons-csv:1.5"
        )
      }
      else {
        packages <- c(
          packages,
          "com.databricks:spark-csv_2.10:1.5.0",
          "org.apache.commons:commons-csv:1.1"
        )
      }
    }

    # add jars to arguments
    if (length(jars) > 0) {
      shell_args <- c(shell_args, "--jars", paste(shQuote(jars, type = shQuoteType), collapse = ","))
    }

    # add packages to arguments
    if (length(packages) > 0) {
      shell_args <- c(shell_args, "--packages", paste(shQuote(packages, type = shQuoteType), collapse = ","))
    }

    # add repositories to arguments
    if (length(repositories) > 0) {
      shell_args <- c(shell_args, "--repositories", paste(repositories, collapse = ","))
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

    if (isBatch) {
      shell_args <- c(shell_args, "--batch", batch)
    }

    # create temp file for stdout and stderr
    output_file <- Sys.getenv("SPARKLYR_LOG_FILE", tempfile(fileext = "_spark.log"))
    error_file <- Sys.getenv("SPARKLYR_LOG_FILE", tempfile(fileext = "_spark.err"))

    console_log <- spark_config_exists(config, "sparklyr.log.console", FALSE)

    stdout_param <- if (console_log) "" else output_file
    stderr_param <- if (console_log) "" else output_file

    start_time <- floor(as.numeric(Sys.time()) * 1000)

    if (spark_config_value(config, "sparklyr.verbose", FALSE)) {
      message(spark_submit_path, " ", paste(shell_args, collapse = " "))
    }

    # start the shell (w/ specified additional environment variables)
    env <- unlist(as.list(environment))
    withr::with_envvar(env, {
      system2(spark_submit_path,
        args = shell_args,
        stdout = stdout_param,
        stderr = stderr_param,
        wait = FALSE
      )
    })

    # support custom operations after spark-submit useful to enable port forwarding
    spark_config_value(config, c("sparklyr.connect.aftersubmit", "sparklyr.events.aftersubmit"))

    # batch connections only use the shell to submit an application, not to connect.
    if (identical(batch, TRUE)) {
      return(NULL)
    }

    # for yarn-cluster
    if (spark_master_is_yarn_cluster(master, config) && is.null(config[["sparklyr.gateway.address"]])) {
      gatewayAddress <- config[["sparklyr.gateway.address"]] <- spark_yarn_cluster_get_gateway(config, start_time)
    }

    # reload port and address to let kubernetes hooks find the right container
    gatewayConfigRetries <- spark_config_value(config, "sparklyr.gateway.config.retries", 10)
    gatewayPort <- as.integer(spark_config_value_retries(config, "sparklyr.gateway.port", "8880", gatewayConfigRetries))
    gatewayAddress <- spark_config_value_retries(config, "sparklyr.gateway.address", "localhost", gatewayConfigRetries)

    while (gateway_connect_attempts > 0) {
      gateway_connect_attempts <- gateway_connect_attempts - 1
      withCallingHandlers(
        {
          # connect and wait for the service to start
          gatewayInfo <- spark_connect_gateway(gatewayAddress,
            gatewayPort,
            sessionId,
            config = config,
            isStarting = TRUE
          )
          break
        },
        error = function(e) {
          if (gateway_connect_attempts > 0) {
            Sys.sleep(gateway_connect_retry_interval_s)
            gateway_connect_retry_interval_s <-
              gateway_connect_retry_interval_s * gateway_connect_retry_interval_multiplier
          } else {
            abort_shell(
              paste(
                "Failed while connecting to sparklyr to port (",
                gatewayPort,
                if (spark_master_is_yarn_cluster(master, config)) {
                  paste0(
                    ") and address (",
                    gatewayAddress
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
          }
        }
      )
    }
  }

  # batch connections only use the shell to submit an application, not to connect.
  if (identical(batch, TRUE)) {
    return(NULL)
  }

  withCallingHandlers(
    {
      interval <- spark_config_value(config, "sparklyr.backend.interval", 1)

      backend <- socketConnection(
        host = gatewayAddress,
        port = gatewayInfo$backendPort,
        server = FALSE,
        blocking = interval > 0,
        open = "wb",
        timeout = interval
      )
      class(backend) <- c(class(backend), "shell_backend")

      monitoring <- socketConnection(
        host = gatewayAddress,
        port = gatewayInfo$backendPort,
        server = FALSE,
        blocking = interval > 0,
        open = "wb",
        timeout = interval
      )
      class(monitoring) <- c(class(monitoring), "shell_backend")
    },
    error = function(err) {
      close(gatewayInfo$gateway)

      abort_shell(
        paste("Failed to open connection to backend:", err$message),
        spark_submit_path,
        shell_args,
        output_file,
        error_file
      )
    }
  )

  # create the shell connection
  sc <- new_spark_shell_connection(list(
    # spark_connection
    master = master,
    method = "shell",
    app_name = app_name,
    config = config,
    state = new.env(),
    extensions = extensions,
    # spark_shell_connection
    spark_home = spark_home,
    backend = backend,
    monitoring = monitoring,
    gateway = gatewayInfo$gateway,
    output_file = output_file,
    sessionId = sessionId,
    home_version = versionSparkHome
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
  clear_jobjs()

  invisible(stop_shell(sc, ...))
}

# Stop the Spark R Shell
stop_shell <- function(sc, terminate = FALSE) {
  terminationMode <- if (terminate == TRUE) "terminateBackend" else "stopBackend"

  # use monitoring connection to terminate
  sc$state$use_monitoring <- TRUE

  invoke_method(
    sc,
    FALSE,
    "Handler",
    terminationMode
  )

  close(sc$backend)
  close(sc$gateway)
  close(sc$monitoring)
}

#' @export
connection_is_open.spark_shell_connection <- function(sc) {
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

#' @export
spark_log.spark_shell_connection <- function(sc, n = 100, filter = NULL, ...) {
  log <- if (.Platform$OS.type == "windows") {
    file("logs/log4j.spark.log")
  } else {
    tryCatch(file(sc$output_file), error = function(e) NULL)
  }

  if (is.null(log)) {
    return("Spark log is not available.")
  } else {
    lines <- readLines(log)
    close(log)
  }

  if (!is.null(filter)) {
    lines <- lines[grepl(filter, lines)]
  }

  linesLog <- if (!is.null(n)) {
    utils::tail(lines, n = n)
  } else {
    lines
  }

  structure(linesLog, class = "spark_log")
}

#' @export
invoke_method.spark_shell_connection <- function(sc, static, object, method, ...) {
  core_invoke_method(sc, static, object, method, FALSE, ...)
}

#' @export
j_invoke_method.spark_shell_connection <- function(sc, static, object, method, ...) {
  core_invoke_method(sc, static, object, method, TRUE, ...)
}

#' @export
print_jobj.spark_shell_connection <- function(sc, jobj, ...) {
  if (connection_is_open(sc)) {
    info <- jobj_info(jobj)
    fmt <- "<jobj[%s]>\n  %s\n  %s\n"
    cat(sprintf(fmt, jobj$id, info$class, info$repr))
    if (identical(info$class, "org.apache.spark.SparkContext")) {
      spark_context_fmt <- "\n  appName: %s\n  master: %s\n  files: %s\n  jars:  %s\n"
      quote_str <- function(str) paste0("'", str, "'")
      cat(sprintf(
        spark_context_fmt,
        invoke(jobj, "appName"),
        invoke(jobj, "master"),
        invoke(jobj, "%>%", list("files"), list("mkString", " ")),
        invoke(jobj, "%>%", list("jars"), list("mkString", " "))
      ))
    }
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
  create_spark_config <- function() {
    # create the spark config
    settings <- list(list("setAppName", sc$app_name))

    if (!spark_master_is_yarn_cluster(sc$master, sc$config) &&
      !spark_master_is_gateway(sc$master)) {
      settings <- c(settings, list(list("setMaster", sc$master)))

      if (!is.null(sc$spark_home)) {
        settings <- c(settings, list(list("setSparkHome", sc$spark_home)))
      }
    }
    conf <- invoke_new(sc, "org.apache.spark.SparkConf") %>% (
      function(obj) do.call(invoke, c(obj, "%>%", settings))
    )

    context_config <- connection_config(sc, "spark.", c("spark.sql."))
    apply_config(conf, context_config, "set", "spark.")

    default_config <- shell_connection_config_defaults()
    default_config_remove <- Filter(function(e) e %in% names(context_config), names(default_config))
    default_config[default_config_remove] <- NULL
    apply_config(conf, default_config, "set", "spark.")

    conf
  }

  # initialize and return the connection
  withCallingHandlers(
    {
      backend <- invoke_static(sc, "sparklyr.Shell", "getBackend")
      sc$state$spark_context <- invoke(backend, "getSparkContext")

      # create the spark config
      conf <- create_spark_config()

      init_hive_ctx_for_spark_2_plus <- function() {
        # For Spark 2.0+, we create a `SparkSession`.
        session_builder <- invoke_static(
          sc,
          "org.apache.spark.sql.SparkSession",
          "builder"
        ) %>%
          invoke("config", conf) %>%
          apply_config(connection_config(sc, "spark.sql."), "config", "spark.sql.")

        if (identical(sc$state$hive_support_enabled, TRUE)) {
          invoke(session_builder, "enableHiveSupport")
        }

        session <- session_builder %>% invoke("getOrCreate")

        # Cache the session as the "hive context".
        sc$state$hive_context <- session

        # Set the `SparkContext`.
        sc$state$spark_context <- sc$state$spark_context %||% invoke(session, "sparkContext")
      }

      if (is.null(spark_context(sc))) {
        # create the spark context and assign the connection to it
        # use spark home version since spark context is not yet initialized in shell connection
        # but spark_home might not be initialized in submit_batch while spark context is available
        if ((!identical(sc$home_version, NULL) && sc$home_version >= "2.0") ||
          (!identical(spark_context(sc), NULL) && spark_version(sc) >= "2.0")) {
          init_hive_ctx_for_spark_2_plus()
        } else {
          sc$state$spark_context <- invoke_static(
            sc,
            "org.apache.spark.SparkContext",
            "getOrCreate",
            conf
          )
          # we might not be aware of the actual version of Spark that is running yet until now
          actual_spark_version <- tryCatch(
            invoke(sc$state$spark_context, "version"),
            error = function(e) {
              warning(paste(
                "Failed to get version from SparkContext:",
                e
              ))
              NULL
            }
          )
          if (!identical(actual_spark_version, NULL) && actual_spark_version >= "2.0") {
            init_hive_ctx_for_spark_2_plus()
          }
        }

        invoke(backend, "setSparkContext", spark_context(sc))
      } else if (is.null(sc$state$hive_context) && spark_version(sc) >= "2.0") {
        init_hive_ctx_for_spark_2_plus()
      }

      # If Spark version is 2.0.0 or above, hive_context should be initialized by now.
      # So if that's not the case, then attempt to initialize it assuming Spark version is below 2.0.0
      sc$state$hive_context <- sc$state$hive_context %||% tryCatch(
        invoke_new(
          sc,
          if (identical(sc$state$hive_support_enabled, TRUE)) {
            "org.apache.spark.sql.hive.HiveContext"
          } else {
            "org.apache.spark.sql.SQLContext"
          },
          sc$state$spark_context
        ),
        error = function(e) {
          warning(e$message)
          warning(
            "Failed to create Hive context, falling back to SQL. Some operations, ",
            "like window-functions, will not work"
          )

          jsc <- invoke_static(
            sc,
            "org.apache.spark.api.java.JavaSparkContext",
            "fromSparkContext",
            sc$state$spark_context
          )

          hive_context <- invoke_static(
            sc,
            "org.apache.spark.sql.api.r.SQLUtils",
            "createSQLContext",
            jsc
          )

          params <- connection_config(sc, "spark.sql.")
          apply_config(hive_context, params, "setConf", "spark.sql.")

          # return hive_context
          hive_context
        }
      )

      # create the java spark context and assign the connection to it
      sc$state$java_context <- invoke_static(
        sc,
        "org.apache.spark.api.java.JavaSparkContext",
        "fromSparkContext",
        spark_context(sc)
      )

      # register necessary sparklyr UDFs
      udf_reg <- invoke(sc$state$hive_context, "udf")
      invoke_static(sc, "sparklyr.UdfUtils", "registerSparklyrUDFs", udf_reg)

      # return the modified connection
      sc
    },
    error = function(e) {
      abort_shell(
        paste("Failed during initialize_connection:", e$message),
        spark_submit_path = NULL,
        shell_args = NULL,
        output_file = sc$output_file,
        error_file = NULL
      )
    }
  )
}

#' @export
invoke.shell_jobj <- function(jobj, method, ...) {
  invoke_method(spark_connection(jobj), FALSE, jobj, method, ...)
}

#' @export
j_invoke.shell_jobj <- function(jobj, method, ...) {
  j_invoke_method(spark_connection(jobj), FALSE, jobj, method, ...)
}

#' @export
invoke_static.spark_shell_connection <- function(sc, class, method, ...) {
  invoke_method(sc, TRUE, class, method, ...)
}

#' @export
j_invoke_static.spark_shell_connection <- function(sc, class, method, ...) {
  j_invoke_method(sc, TRUE, class, method, ...)
}

#' @export
invoke_new.spark_shell_connection <- function(sc, class, ...) {
  invoke_method(sc, TRUE, class, "<init>", ...)
}

#' @export
j_invoke_new.spark_shell_connection <- function(sc, class, ...) {
  j_invoke_method(sc, TRUE, class, "<init>", ...)
}
