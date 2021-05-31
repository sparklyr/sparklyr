# nocov start
#' @include shell_connection.R

create_hive_context.livy_connection <- function(sc) {
  invoke_new(
    sc,
    "org.apache.spark.sql.hive.HiveContext",
    spark_context(sc)
  )
}

#' @import httr
#' @importFrom httr http_error
#' @importFrom httr http_status
#' @importFrom httr text_content
livy_validate_http_response <- function(message, req) {
  if (http_error(req)) {
    if (isTRUE(all.equal(status_code(req), 401))) {
      stop("Livy operation is unauthorized. Try spark_connect with config = livy_config()")
    }
    else {
      httpStatus <- http_status(req)
      httpContent <- content(req, as = "text", encoding = "UTF-8")
      stop(message, " (", httpStatus$message, "): ", httpContent)
    }
  }
}

livy_available_jars <- function() {
  system.file("java", package = "sparklyr") %>%
    dir(pattern = "sparklyr") %>%
    gsub("^sparklyr-|-.*\\.jar", "", .)
}

#' Create a Spark Configuration for Livy
#'
#' @export
#'
#' @importFrom base64enc base64encode
#' @importFrom jsonlite unbox
#'
#' @param config Optional base configuration
#' @param username The username to use in the Authorization header
#' @param password The password to use in the Authorization header
#' @param negotiate Whether to use gssnegotiate method or not
#' @param custom_headers List of custom headers to append to http requests. Defaults to \code{list("X-Requested-By" = "sparklyr")}.
#' @param proxy Either NULL or a proxy specified by httr::use_proxy(). Defaults to NULL.
#' @param curl_opts List of CURL options (e.g., verbose, connecttimeout, dns_cache_timeout, etc, see httr::httr_options() for a
#'   list of valid options) -- NOTE: these configurations are for libcurl only and separate from HTTP headers or Livy session
#'   parameters.
#' @param ... additional Livy session parameters
#'
#' @details
#'
#' Extends a Spark \code{spark_config()} configuration with settings
#' for Livy. For instance, \code{username} and \code{password}
#' define the basic authentication settings for a Livy session.
#'
#' The default value of \code{"custom_headers"} is set to \code{list("X-Requested-By" = "sparklyr")}
#' in order to facilitate connection to Livy servers with CSRF protection enabled.
#'
#' Additional parameters for Livy sessions are:
#' \describe{
#'   \item{\code{proxy_user}}{User to impersonate when starting the session}
#'   \item{\code{jars}}{jars to be used in this session}
#'   \item{\code{py_files}}{Python files to be used in this session}
#'   \item{\code{files}}{files to be used in this session}
#'   \item{\code{driver_memory}}{Amount of memory to use for the driver process}
#'   \item{\code{driver_cores}}{Number of cores to use for the driver process}
#'   \item{\code{executor_memory}}{Amount of memory to use per executor process}
#'   \item{\code{executor_cores}}{Number of cores to use for each executor}
#'   \item{\code{num_executors}}{Number of executors to launch for this session}
#'   \item{\code{archives}}{Archives to be used in this session}
#'   \item{\code{queue}}{The name of the YARN queue to which submitted}
#'   \item{\code{name}}{The name of this session}
#'   \item{\code{heartbeat_timeout}}{Timeout in seconds to which session be orphaned}
#'   \item{\code{conf}}{Spark configuration properties (Map of key=value)}
#' }
#'
#' Note that \code{queue} is supported only by version 0.4.0 of Livy or newer.
#' If you are using the older one, specify queue via \code{config} (e.g.
#' \code{config = spark_config(spark.yarn.queue = "my_queue")}).
#'
#' @return Named list with configuration data
livy_config <- function(config = spark_config(),
                        username = NULL,
                        password = NULL,
                        negotiate = FALSE,
                        custom_headers = list("X-Requested-By" = "sparklyr"),
                        proxy = NULL,
                        curl_opts = NULL,
                        ...) {
  additional_params <- list(...)

  if (negotiate) {
    config[["sparklyr.livy.auth"]] <- httr::authenticate("", "", type = "gssnegotiate")
  } else if (!is.null(username) || !is.null(password)) {
    config[["sparklyr.livy.auth"]] <- httr::authenticate(username, password, type = "basic")
  }

  if (!is.null(custom_headers)) {
    for (l in names(custom_headers)) {
      config[["sparklyr.livy.headers"]] <- c(
        config[["sparklyr.livy.headers"]], custom_headers[l]
      )
    }
  }

  if (!is.null(proxy)) config[["sparklyr.livy.proxy"]] <- proxy

  if (!is.null(curl_opts)) config[["sparklyr.livy.curl_opts"]] <- curl_opts

  if (length(additional_params) > 0) {
    # snake_case to camelCase mapping for allowed Livy params
    params_map <- c(
      proxy_user = "proxyUser",
      jars = "jars",
      py_files = "pyFiles",
      files = "files",
      driver_memory = "driverMemory",
      driver_cores = "driverCores",
      executor_memory = "executorMemory",
      executor_cores = "executorCores",
      num_executors = "numExecutors",
      archives = "archives",
      queue = "queue",
      name = "name",
      heartbeat_timeout = "heartbeatTimeoutInSecond",
      conf = "conf"
    )

    # Params need to be restricted or livy will complain about unknown parameters
    allowed_params <- names(params_map)

    valid_params <- names(additional_params) %in% allowed_params
    if (!all(valid_params)) {
      stop(paste0(names(additional_params[!valid_params]), sep = ", "), " are not valid session parameters. Valid parameters are: ", paste0(allowed_params, sep = ", "))
    }
    singleValues <- c("proxy_user", "driver_memory", "driver_cores", "executor_memory", "executor_cores", "num_executors", "queue", "name", "heartbeat_timeout")
    singleValues <- singleValues[singleValues %in% names(additional_params)]
    additional_params[singleValues] <- lapply(additional_params[singleValues], unbox)


    for (l in names(additional_params)) {
      # Parse the params names from snake_case to camelCase
      config[[paste0("livy.", params_map[[l]])]] <- additional_params[[l]]
    }
  }
  config
}

livy_get_httr_config <- function(config, headers) {
  httr_config <- list()
  headers <- c(headers, config[["sparklyr.livy.headers"]])
  if (length(headers) > 0) {
    httr_config <- do.call(add_headers, headers)
  }

  proxy <- config[["sparklyr.livy.proxy"]]
  httr_config$options <- c(httr_config$options, proxy$options)

  curl_opts <- config[["sparklyr.livy.curl_opts"]]
  httr_config$options <- c(httr_config$options, curl_opts)

  httr_config
}

#' @importFrom httr GET
livy_get_json <- function(url, config) {
  req <- GET(url,
    config = livy_get_httr_config(config, list(
      "Content-Type" = "application/json"
    )),
    config$sparklyr.livy.auth
  )

  livy_validate_http_response("Failed to retrieve livy session", req)

  content(req)
}

#' @import assertthat
livy_get_sessions <- function(master, config) {
  sessions <- livy_get_json(paste(master, "sessions", sep = "/"), config)

  assert_that(!is.null(sessions$sessions))
  assert_that(!is.null(sessions$total))

  sessions
}

#' @importFrom jsonlite unbox
livy_config_get_prefix <- function(master, config, prefix, not_prefix) {
  params <- connection_config(list(
    master = master,
    config = config
  ), prefix, not_prefix)

  params <- lapply(params, function(param) {
    if (length(param) == 1) unbox(param) else param
  })

  if (length(params) == 0) {
    NULL
  } else {
    names(params) <- paste(prefix, names(params), sep = "")
    params
  }
}

#' @importFrom jsonlite toJSON
livy_config_get <- function(master, config) {
  sparkConfig <- livy_config_get_prefix(master, config, "spark.", c("spark.sql."))
  c(sparkConfig)
}

#' @importFrom httr POST
#' @importFrom jsonlite unbox
livy_create_session <- function(master, config) {
  data <- list(
    kind = unbox("spark"),
    conf = livy_config_get(master, config)
  )

  session_params <- connection_config(list(master = master, config = config), "livy.", "livy.session.")
  if (length(session_params) > 0) data <- append(data, session_params)

  req <- POST(paste(master, "sessions", sep = "/"),
    config = livy_get_httr_config(config, list(
      "Content-Type" = "application/json"
    )),
    body = toJSON(
      data
    ),
    config$sparklyr.livy.auth
  )

  livy_validate_http_response("Failed to create livy session", req)

  content <- content(req)

  assert_that(!is.null(content$id))
  assert_that(!is.null(content$state))
  assert_that(content$kind == "spark")

  content
}

livy_destroy_session <- function(sc) {
  req <- DELETE(paste(sc$master, "sessions", sc$sessionId, sep = "/"),
    config = livy_get_httr_config(sc$config, list(
      "Content-Type" = "application/json"
    )),
    body = NULL,
    sc$config$sparklyr.livy.auth
  )

  livy_validate_http_response("Failed to destroy livy statement", req)

  content <- content(req)
  assert_that(content$msg == "deleted")

  NULL
}

livy_get_session <- function(sc) {
  session <- livy_get_json(paste(sc$master, "sessions", sc$sessionId, sep = "/"), sc$config)

  assert_that(!is.null(session$state))
  assert_that(session$id == sc$sessionId)

  session
}

livy_code_new_return_var <- function(sc) {
  totalReturnVars <- sc$code$totalReturnVars
  name <- paste("sparklyrRetVar", totalReturnVars, sep = "_")
  sc$code$totalReturnVars <- totalReturnVars + 1

  name
}

livy_jobj_create <- function(sc, varName) {
  structure(
    list(
      sc = sc,
      varName = varName,
      varType = NULL,
      response = NULL
    ),
    class = c("spark_jobj", "livy_jobj")
  )
}

livy_statement_new <- function(code, lobj) {
  list(
    code = code,
    lobj = lobj
  )
}

livy_serialized_chunks <- function(serialized, n) {
  num_chars <- nchar(serialized)
  start <- seq(1, num_chars, by = n)
  sapply(seq_along(start), function(i) {
    end <- if (i < length(start)) start[i + 1] - 1 else num_chars
    substr(serialized, start[i], end)
  })
}

livy_statement_compose <- function(sc, static, class, method, ...) {
  serialized <- livy_invoke_serialize(sc = sc, static = static, object = class, method = method, ...)
  chunks <- livy_serialized_chunks(serialized, 10000)

  chunk_vars <- list()
  last_var <- NULL
  var_name <- "sparklyr_return"

  if (length(chunks) == 1) {
    last_var <- paste("\"", chunks[1], "\"", sep = "")
  }
  else {
    last_var <- "builder.toString"
    chunk_vars <- c(chunk_vars, "val builder = StringBuilder.newBuilder")
    for (i in seq_along(chunks)) {
      chunk_vars <- c(chunk_vars, paste("builder.append(\"", chunks[i], "\") == \"\"", sep = ""))
    }
  }

  var_name <- livy_code_new_return_var(sc)

  invoke_var <- paste0(
    "var ", var_name, " = sparklyr.LivyUtils.invokeFromBase64(", last_var, ")"
  )

  code <- paste(
    c(
      chunk_vars,
      invoke_var
    ),
    collapse = "\n"
  )

  livy_statement_new(
    code = code,
    lobj = livy_jobj_create(sc, var_name)
  )
}

livy_statement_compose_magic <- function(lobj, magic) {
  code <- paste(
    "%",
    magic,
    " ",
    lobj$varName,
    sep = ""
  )

  livy_statement_new(
    code = code,
    lobj = NULL
  )
}

livy_get_statement <- function(sc, statementId) {
  statement <- livy_get_json(
    paste(sc$master, "sessions", sc$sessionId, "statements", statementId, sep = "/"),
    sc$config
  )

  assert_that(!is.null(statement$state))
  assert_that(statement$id == statementId)

  statement
}

livy_inspect <- function(lobj) {

}

livy_log_operation <- function(sc, text) {
  write(strtrim(text, 200), file = sc$log, append = TRUE)
}

#' @importFrom httr POST
#' @importFrom jsonlite toJSON
#' @importFrom jsonlite unbox
livy_post_statement <- function(sc, code) {
  livy_log_operation(sc, code)

  req <- POST(paste(sc$master, "sessions", sc$sessionId, "statements", sep = "/"),
    config = livy_get_httr_config(sc$config, list(
      "Content-Type" = "application/json"
    )),
    body = toJSON(
      list(
        code = unbox(code)
      )
    ),
    sc$config$sparklyr.livy.auth
  )

  livy_validate_http_response("Failed to invoke livy statement", req)

  statementReponse <- content(req)
  assert_that(!is.null(statementReponse$id))

  waitTimeout <- spark_config_value(sc$config, "livy.session.command.timeout", 30 * 24 * 60 * 60)
  pollInterval <- spark_config_value(sc$config, "livy.session.command.interval", 5)

  commandStart <- Sys.time()

  sleepTime <- 0.001
  while ((statementReponse$state == "running" || statementReponse$state == "waiting" ||
    (statementReponse$state == "available" && is.null(statementReponse$output))) &&
    Sys.time() < commandStart + waitTimeout) {
    statementReponse <- livy_get_statement(sc, statementReponse$id)

    Sys.sleep(sleepTime)

    sleepTime <- min(pollInterval, sleepTime * 2)
  }

  if (statementReponse$state != "available") {
    stop("Failed to execute Livy statement with state ", statementReponse$state)
  }

  assert_that(!is.null(statementReponse$output))

  if (statementReponse$output$status == "error") {
    withr::with_options(list(
      warning.length = 8000
    ), {
      stop(
        "Failed to execute Livy statement with error: ",
        if (is.null(statementReponse$output$evalue)) {
          jsonlite::toJSON(statementReponse)
        } else {
          statementReponse$output$evalue
        },
        "\nTraceback: ",
        paste(statementReponse$output$traceback, collapse = "")
      )
    })
  }

  data <- statementReponse$output$data

  if ("text/plain" == names(data)[[1]]) {
    livy_log_operation(sc, "\n")
    livy_log_operation(sc, data[[1]])
    livy_log_operation(sc, "\n")
  }

  data
}

livy_invoke_statement <- function(sc, statement) {
  data <- livy_post_statement(sc, statement$code)
  assert_that(!is.null(data))

  supportedDataTypes <- list(
    "text/plain" = list(
      dataToResult = function(data) {
      }
    ),
    "application/json" = list(
      dataToResult = function(data) {
        data
      }
    )
  )

  assert_that(length(data) == 1)
  dataType <- names(data)[[1]]
  data <- data[[1]]

  if (!dataType %in% names(supportedDataTypes)) {
    stop("Livy statement with output type ", dataType, " is unsupported")
  }

  result <- supportedDataTypes[[dataType]]$dataToResult(data)
  result
}

livy_invoke_statement_command <- function(sc, static, jobj, method, ...) {
  if (identical(method, "<init>")) {
    paste0("// invoke_new(sc, '", jobj, "', ...)")
  } else if (is.character(jobj)) {
    paste0("// invoke_static(sc, '", jobj, "', '", method, "', ...)")
  } else {
    paste0("// invoke(sc, <jobj>, '", method, "', ...)")
  }
}

livy_invoke_statement_fetch <- function(sc, static, jobj, method, ...) {
  statement <- livy_statement_compose(sc, static, jobj, method, ...)

  # Note: Spark 2.0 requires magic to be present in the statement with the definition.
  statement$code <- paste(
    paste(
      livy_invoke_statement_command(sc, static, jobj, method, ...),
      statement$code,
      sep = "\n"
    ),
    livy_statement_compose_magic(statement$lobj, "json")$code,
    sep = "\n"
  )

  result <- livy_invoke_statement(sc, statement)

  if (!is.character(result)) {
    stop("Failed to execute statement, character result expected but ", typeof(result), " was received.")
  }

  # If result is too long that was truncated, retry with livy magic instead
  if (grepl("\\.\\.\\.$", result)) {
    statement <- livy_statement_compose_magic(statement$lobj, "json")
    result <- livy_invoke_statement(sc, statement)
  }

  lobj <- livy_invoke_deserialize(sc, result)
  lobj
}

livy_try_get_session <- function(sc) {
  session <- NULL
  tryCatch(
    {
      session <- livy_get_session(sc)
    },
    error = function(e) {}
  )

  session
}

livy_validate_master <- function(master, config) {
  retries <- 5
  retriesErr <- NULL
  while (retries >= 0) {
    if (!is.null(retriesErr)) Sys.sleep(1)

    retriesErr <- tryCatch(
      {
        livy_get_sessions(master, config)
        NULL
      },
      error = function(err) {
        err
      }
    )

    if (is.null(retriesErr)) {
      return(NULL)
    }

    retries <- retries - 1
  }

  stop("Failed to connect to Livy service at ", master, ". ", retriesErr$message)
}

livy_connection_not_used_warn <- function(value, default = NULL, name = deparse(substitute(value))) {
  if (!identical(value, default)) {
    warning("Livy connections do not support ", name, " parameter")
  }
}

livy_connection_jars <- function(config, version, scala_version) {
  livy_jars <- as.list(spark_config_value(config, "sparklyr.livy.jar", NULL))

  if (length(livy_jars) == 0) {
    major_version <- gsub("\\.$", "", version)

    livy_jars <- livy_available_jars()
    livy_max_version <- max(numeric_version(livy_jars[livy_jars != "master"]))

    previouis_versions <- Filter(
      function(maybe_version) maybe_version <= major_version,
      numeric_version(gsub("master", paste(livy_max_version, "1", sep = "."), livy_available_jars()))
    )

    target_version <- previouis_versions[length(previouis_versions)]

    target_jar_pattern <- (
      if (is.null(scala_version)) {
        paste0("sparklyr-", target_version)
      } else {
        paste0("sparklyr-", target_version, "-", scala_version)
      })
    target_jar <- dir(system.file("java", package = "sparklyr"), pattern = target_jar_pattern)
    # Select the jar file built with the lowest version of Scala in case there is no
    # requirement for Scala version compatibility
    if (length(target_jar) > 1) {
      target_jar <- stringr::str_sort(target_jar)[[1]]
    }

    livy_branch <- spark_config_value(config, "sparklyr.livy.branch", "feature/sparklyr-1.6.3")

    livy_jars <- paste0(
      "https://github.com/sparklyr/sparklyr/blob/",
      livy_branch,
      "/inst/java/",
      target_jar,
      "?raw=true"
    )
  }

  livy_jars
}

livy_connection <- function(master,
                            config,
                            app_name,
                            version,
                            hadoop_version,
                            extensions,
                            scala_version = NULL) {
  if (is.null(version)) {
    stop("Livy connections now require the Spark version to be specified.", call. = FALSE)
  }

  livy_connection_not_used_warn(app_name, "sparklyr")
  livy_connection_not_used_warn(hadoop_version)
  livy_connection_not_used_warn(extensions, registered_extensions())

  if (grepl("^local(\\[[0-9]*\\])?$", master)) {
    master <- "http://localhost:8998"
  }

  # normalize url by remove trailing /
  master <- gsub("[/]$", "", master)

  livy_validate_master(master, config)

  extensions <- spark_dependencies_from_extensions(version, scala_version, extensions, config)

  config[["livy.jars"]] <- unique(as.character(c(
    config[["livy.jars"]],
    livy_connection_jars(config, version, scala_version),
    extensions$catalog_jars
  )))

  config[["spark.jars.packages"]] <- paste(c(config[["spark.jars.packages"]], extensions$packages), collapse = ",")
  config[["spark.jars.repositories"]] <- paste(c(config[["spark.jars.repositories"]], extensions$repositories), collapse = ",")

  session <- livy_create_session(master, config)

  sc <- new_livy_connection(list(
    # spark_connection
    master = master,
    method = "livy",
    app_name = app_name,
    config = config,
    state = new.env(),
    extensions = extensions,
    # livy_connection
    sessionId = session$id,
    code = new.env(),
    log = tempfile(fileext = ".log")
  ))

  sc$code$totalReturnVars <- 0

  waitStartTimeout <- spark_config_value(config, c("sparklyr.connect.timeout", "livy.session.start.timeout"), 60)
  waitStartReties <- waitStartTimeout * 10
  while (session$state == "starting" &&
    session$state != "dead" &&
    waitStartReties > 0) {
    session <- livy_get_session(sc)

    Sys.sleep(0.1)
    waitStartReties <- waitStartReties - 1
  }

  if (session$state == "starting") {
    stop(
      "Failed to launch livy session, session status is",
      " still starting after waiting for ", waitStartTimeout, " seconds"
    )
  }

  if (session$state != "idle") {
    stop("Failed to launch livy session, session status is ", session$state)
  }

  # stop connection on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      spark_disconnect(sc, terminate = TRUE)
    }
  }, onexit = TRUE)

  sc
}

livy_states_info <- function() {
  list(
    "not_started"   = list(connected = FALSE),
    "starting"      = list(connected = TRUE),
    "recovering"    = list(connected = TRUE),
    "idle"          = list(connected = TRUE),
    "running"       = list(connected = TRUE),
    "busy"          = list(connected = TRUE),
    "shutting_down" = list(connected = TRUE),
    "error"         = list(connected = TRUE),
    "dead"          = list(connected = FALSE),
    "success"       = list(connected = TRUE)
  )
}

#' @export
spark_log.livy_connection <- function(sc, n = 100, filter = NULL, ...) {
  stop("Unsupported operation for livy connections")
}

#' @export
spark_web.livy_connection <- function(sc, ...) {
  stop("Unsupported operation for livy connections")
}

#' @export
connection_is_open.livy_connection <- function(sc) {
  session <- livy_try_get_session(sc)
  if (is.null(session)) {
    FALSE
  }
  else {
    stateInfo <- livy_states_info()[[session$state]]

    assert_that(!is.null(stateInfo))

    stateInfo$connected
  }
}

#' @export
spark_disconnect.livy_connection <- function(sc, ...) {
  terminate <- list(...)$terminate
  if (!identical(terminate, FALSE)) {
    invisible(livy_destroy_session(sc))
  }
}

#' @export
print_jobj.livy_connection <- print_jobj.spark_shell_connection

#' @export
invoke.livy_jobj <- function(jobj, method, ...) {
  livy_invoke_statement_fetch(spark_connection(jobj), FALSE, jobj, method, ...)
}

#' @export
invoke_static.livy_connection <- function(sc, class, method, ...) {
  livy_invoke_statement_fetch(sc, TRUE, class, method, ...)
}

#' @export
invoke_new.livy_connection <- function(sc, class, ...) {
  livy_invoke_statement_fetch(sc, TRUE, class, "<init>", ...)
}

invoke_raw <- function(sc, code, ...) {
  livy_post_statement(sc, code)
}

#' @export
initialize_connection.livy_connection <- function(sc) {
  withCallingHandlers(
    {
      session <- tryCatch(
        {
          invoke_static(
            sc,
            "org.apache.spark.sql.SparkSession",
            "builder"
          ) %>%
            invoke("%>%", list("enableHiveSupport"), list("getOrCreate"))
        },
        error = function(e) {
          NULL
        }
      )

      sc$state$spark_context <- tryCatch(
        {
          invoke(session, "sparkContext")
        },
        error = function(e) {
          invoke_static(
            sc,
            "org.apache.spark.SparkContext",
            "getOrCreate"
          )
        }
      )

      sc$state$java_context <- invoke_static(
        sc,
        "org.apache.spark.api.java.JavaSparkContext",
        "fromSparkContext",
        spark_context(sc)
      )

      # cache spark version
      sc$state$spark_version <- spark_version(sc)

      sc$state$hive_context <- session %||% create_hive_context(sc)

      if (spark_version(sc) < "2.0.0") {
        params <- connection_config(sc, "spark.sql.")
        apply_config(hive_context, params, "setConf", "spark.sql.")
      }

      sc
    },
    error = function(err) {
      stop(
        "Failed to initialize livy connection: ",
        err$message,
        "\n\ncallstack:\n",
        paste(sys.calls(), collapse = "\n")
      )
    }
  )
}

# nocov end
