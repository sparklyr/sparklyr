#' @import assertthat
livy_get_sessions <- function(master) {
  sessions <- fromJSON(paste(master, "sessions", sep = "/"))

  assert_that(!is.null(sessions$sessions))
  assert_that(!is.null(sessions$total))

  sessions
}

#' @import httr
#' @import jsonlite
livy_create_session <- function(master) {
  data <- list(kind = "spark")

  req <- POST(paste(master, "sessions", sep = "/"),
    add_headers(
      "Content-Type" = "application/json"
    ),
    body = toJSON(
      list(
        kind = unbox("spark")
      )
    )
  )

  if (httr::http_error(req)) {
    stop("Failed to create livy session: ", content(req))
  }

  content <- content(req)

  assert_that(!is.null(content$id))
  assert_that(!is.null(content$state))
  assert_that(content$kind == "spark")

  content
}

livy_destroy_session <- function(sc) {
  req <- DELETE(paste(sc$master, "sessions", sc$sessionId, sep = "/"),
    add_headers(
      "Content-Type" = "application/json"
    ),
    body = NULL
  )

  if (httr::http_error(req)) {
    stop("Failed to destroy livy session: ", content(req))
  }

  content <- content(req)
  assert_that(content$msg == "deleted")

  NULL
}

livy_get_session <- function(sc) {
  session <- fromJSON(paste(sc$master, "sessions", sc$sessionId, sep = "/"))

  assert_that(!is.null(session$state))
  assert_that(session$id == sc$sessionId)

  session
}

livy_code_quote_parameters <- function(parameters) {
  paste("\"", parameters, "\"", sep = "")
}

livy_code_new_return_var <- function(sc) {
  totalReturnVars <- sc$code$totalReturnVars
  name <- paste("sparklyrRetVar", totalReturnVars, sep = "_")
  sc$code$totalReturnVars <- totalReturnVars + 1

  name
}

livy_code_compose_static <- function(sc, class, method, parameters) {
  varName <- livy_code_new_return_var(sc)

  paste(
    "var ", varName, " = ",
    class,
    if (is.null(class)) "" else paste(class, ".", sep = ""),
    method,
    "(",
    paste(
      livy_code_quote_parameters(parameters),
      sep = ","
    ),
    ")",
    sep = ""
  )

  list(
    varName = varName
  )
}

livy_code_compose_method <- function(jobj, method, parameters) {
  varName <- livy_code_get_return_var(sc)

  paste(
    "var ", varName, " = ",
    class,
    jobj$varName,
    ".",
    method,
    "(",
    paste(
      livy_code_quote_parameters(parameters),
      sep = ","
    ),
    ")",
    sep = ""
  )

  list(
    varName = varName
  )
}

livy_code_compose_new <- function(sc, class, parameters) {
  varName <- livy_code_new_return_var(sc)

  paste(
    "var ", varName, " = new ",
    class,
    if (is.null(class)) "" else class,
    method,
    "(",
    paste(
      livy_code_quote_parameters(parameters),
      sep = ","
    ),
    ")",
    sep = ""
  )

  list(
    varName = varName
  )
}

livy_get_statement <- function(sc, statementId) {
  statement <- fromJSON(paste(sc$master, "sessions", sc$sessionId, "statements", statementId, sep = "/"))

  assert_that(!is.null(statement$state))
  assert_that(statement$id == statementId)

  statement
}

livy_invoke_code <- function(sc, scala) {
  req <- POST(paste(sc$master, "sessions", sc$sessionId, "statements", sep = "/"),
    add_headers(
      "Content-Type" = "application/json"
    ),
    body = toJSON(
      list(
        code = unbox(scala)
      )
    )
  )

  if (httr::http_error(req)) {
    stop("Failed to invoke livy statement: ", content(req))
  }

  statement <- content(req)
  assert_that(!is.null(statement$id))

  waitTimeout <- spark_config_value(sc$config, "livy.session.command.timeout", 60)
  waitTimeout <- waitTimeout * 10
  while (statement$state == "running" &&
         waitTimeout > 0) {
    statement <- livy_get_statement(sc, statement$id)

    Sys.sleep(0.1)
    waitTimeout <- waitTimeout - 1
  }

  if (statement$state != "available") {
    stop("Failed to execute Livy statement with state ", statement$state)
  }

  assert_that(!is.null(statement$output))
  assert_that(!is.null(statement$output$data))

  if (!"text/plain" %in% names(statement$output$data)) {
    stop("Livy statement with output type", statement$output$data[[1]], "is unsupported")
  }

  statement$output$data$`text/plain`
}

livy_try_get_session <- function(sc) {
  session <- NULL
  tryCatch({
    session <- livy_get_session(sc)
  }, error = function(e) {})

  session
}

livy_validate_master <- function(master) {
  tryCatch({
    livy_get_sessions(master)
  }, error = function(err) {
    stop("Failed to connect to Livy service at ", master, ". ", err$message)
  })

  NULL
}

#' @import jsonlite
livy_connection <- function(master, config) {
  livy_validate_master(master)

  session <- livy_create_session(master)

  sc <- structure(class = c("spark_connection", "livy_connection"), list(
    master = master,
    sessionId = session$id,
    config = config,
    code = new.env()
  ))

  sc$code$totalReturnVars <- 0

  waitStartTimeout <- spark_config_value(config, "livy.session.start.timeout", 60)
  waitStartReties <- waitStartTimeout * 10
  while (session$state == "starting" &&
         session$state != "dead" &&
         waitStartReties > 0) {
    session <- livy_get_session(sc)

    Sys.sleep(0.1)
    waitStartReties <- waitStartReties - 1
  }

  if (session$state == "starting") {
    stop("Failed to launch livy session, session status is still starting after waiting for ", waitStartTimeout, " seconds")
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
  if (terminate) {
    livy_destroy_session(sc)
  }
}

#' @export
invoke.livy_connection <- function(jobj, method, ...) {
  statement <- livy_code_compose_method(sc, class, method, ...)
  livy_invoke_code(sc, statement)
}

#' @export
invoke_static.livy_connection <- function(sc, class, method, ...) {
  statement <- livy_code_compose_static(sc, class, method, ...)
  livy_invoke_code(sc, statement)
}

#' @export
invoke_new.livy_connection <- function(sc, class, ...) {
  statement <- livy_code_compose_new(sc, class, ...)
  livy_invoke_code(sc, statement)
}

#' @export
initialize_connection.livy_connection <- function(sc) {
  tryCatch({
    conf <- invoke_new(sc, "org.apache.spark.SparkConf")
    conf <- invoke(conf, "setAppName", sc$app_name)
    conf <- invoke(conf, "setMaster", sc$master)
    conf <- invoke(conf, "setSparkHome", sc$spark_home)

    sc$spark_context <- sc$spark_context <- invoke_static(
      sc,
      "org.apache.spark.SparkContext",
      "getOrCreate",
      conf
    )
  }, error = function(err) {
    stop("Failed to initialize livy connection: ", err$message)
  })
}
