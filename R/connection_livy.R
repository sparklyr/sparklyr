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

livy_code_quote_parameters <- function(params) {
  if (length(params) == 0) {
    ""
  } else {
    params <- lapply(params, function(param) {
      paramClass <- class(param)
      if (paramClass == "character") {
        paste("\"", param, "\"", sep = "")
      }
      else if (paramClass == "spark_lobj") {
        param$varName
      }
      else if (is.numeric(param)) {
        param
      }
      else {
        stop("Unsupported parameter ", param, " of class ", paramClass, " detected")
      }
    })

    paste(params, collapse = ",")
  }
}

livy_code_new_return_var <- function(sc) {
  totalReturnVars <- sc$code$totalReturnVars
  name <- paste("sparklyrRetVar", totalReturnVars, sep = "_")
  sc$code$totalReturnVars <- totalReturnVars + 1

  name
}

livy_lobj_create <- function(sc, varName) {
  structure(
    list(
      sc = sc,
      varName = varName,
      response = NULL
    ),
    class = "spark_lobj"
  )
}

livy_code_method_parameters <- function(parameters) {
  scalaParams <- livy_code_quote_parameters(parameters)

  if (nchar(scalaParams) > 0) {
    paste(
      "(",
      scalaParams,
      ")", sep = ""
    )
  } else {
    # scala method calls do not require () which sparklyrwe need to omit
    # to be compatible with the shell_connection/invoke api which
    # does not make a distinction between calling properties and
    # rity-0 function calls.
    ""
  }
}

livy_statement_compose <- function(code, lobj) {
  list(
    code = code,
    lobj = lobj
  )
}

livy_statement_compose_static <- function(sc, class, method, ...) {
  parameters <- list(...)
  varName <- livy_code_new_return_var(sc)

  code <- paste(
    "var ", varName, " = ",
    if (is.null(class)) "" else paste(class, ".", sep = ""),
    method,
    livy_code_method_parameters(parameters),
    sep = ""
  )

  livy_statement_compose(
    code = code,
    lobj = livy_lobj_create(sc, varName)
  )
}

livy_statement_compose_method <- function(lobj, method, ...) {
  parameters <- list(...)
  varName <- livy_code_new_return_var(lobj$sc)

  code <- paste(
    "var ", varName, " = ",
    lobj$varName,
    ".",
    method,
    livy_code_method_parameters(parameters),
    sep = ""
  )

  livy_statement_compose(
    code = code,
    lobj = livy_lobj_create(lobj$sc, varName)
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

  livy_statement_compose(
    code = code,
    lobj = NULL
  )
}

livy_statement_compose_new <- function(sc, class, ...) {
  parameters <- list(...)
  varName <- livy_code_new_return_var(sc)

  code <- paste(
    "var ", varName, " = new ",
    class,
    livy_code_method_parameters(parameters),
    sep = ""
  )

  livy_statement_compose(
    code = code,
    lobj = livy_lobj_create(sc, varName)
  )
}

livy_statement_parse_response <- function(text, lobj) {
  parsed <- regmatches(text, regexec("([^:]+): (Array)?\\[?([a-zA-Z0-9.]+)\\]? = (.*)", text))
  if (length(parsed) != 1) {
    stop("Failed to parse statement reponse: ", text)
  }

  parsed <- parsed[[1]]
  if (length(parsed) != 5) {
    stop("Failed to parse statement reponse: ", text)
  }

  varName <- parsed[[2]]
  collectionType <- parsed[[3]]
  scalaType <- parsed[[4]]
  scalaValue <- parsed[[5]]

  if (nchar(collectionType) > 0 && collectionType != "Array") {
    stop("Failed to parse statement, unrecognized collection type: ", text)
  }

  livyToRTypeMap <- list(
    "String" = list(
      type = "character",
      parse = function(e) e
    ),
    "Int" = list(
      type = "integer",
      parse = function(e) as.integer(e)
    )
  )

  type <- "object"
  value <- scalaValue

  if (scalaType %in% names(livyToRTypeMap)) {
    livyToRTypeMapInst <- livyToRTypeMap[[scalaType]]
    type <- livyToRTypeMapInst$type
    value <- livyToRTypeMapInst$parse(scalaValue)
  }

  if (collectionType == "Array") {
    lobj$collection = list(
      type = "array",
      entries = type
    )
  }

  if (type == "object") lobj else value
}

livy_get_statement <- function(sc, statementId) {
  statement <- fromJSON(paste(sc$master, "sessions", sc$sessionId, "statements", statementId, sep = "/"))

  assert_that(!is.null(statement$state))
  assert_that(statement$id == statementId)

  statement
}

livy_inspect <- function(lobj) {

}

livy_invoke_statement <- function(sc, statement) {
  req <- POST(paste(sc$master, "sessions", sc$sessionId, "statements", sep = "/"),
    add_headers(
      "Content-Type" = "application/json"
    ),
    body = toJSON(
      list(
        code = unbox(statement$code)
      )
    )
  )

  if (httr::http_error(req)) {
    stop("Failed to invoke livy statement: ", content(req))
  }

  statementReponse <- content(req)
  assert_that(!is.null(statementReponse$id))

  waitTimeout <- spark_config_value(sc$config, "livy.session.command.timeout", 60)
  waitTimeout <- waitTimeout * 10
  while (statementReponse$state == "running" &&
         waitTimeout > 0) {
    statementReponse <- livy_get_statement(sc, statementReponse$id)

    Sys.sleep(0.1)
    waitTimeout <- waitTimeout - 1
  }

  if (statementReponse$state != "available") {
    stop("Failed to execute Livy statement with state ", statementReponse$state)
  }

  assert_that(!is.null(statementReponse$output))

  if (statementReponse$output$status == "error") {
    stop("Failed to execute Livy statement with error: ", statementReponse$output$evalue)
  }

  assert_that(!is.null(statementReponse$output$data))

  supportedDataTypes <- list(
    "text/plain" = list(
      dataToResult = function(data) {
        livy_statement_parse_response(data, statement$lobj)
      }
    ),
    "application/json" = list(
      dataToResult = function(data) {
        data
      }
    )
  )

  data <- statementReponse$output$data
  assert_that(length(data) == 1)
  dataType <- names(data)[[1]]
  data <- data[[1]]

  if (!dataType %in% names(supportedDataTypes)) {
    stop("Livy statement with output type ", dataType, " is unsupported")
  }

  result <- supportedDataTypes[[dataType]]$dataToResult(data)
  result
}

livy_invoke_statement_fetch <- function(sc, statement) {
  result <- livy_invoke_statement(sc, statement)

  if (class(result) == "spark_lobj" && !is.null(result$collection)) {
    if (result$collection$entries == "object") {
      lengthStatement <- livy_statement_compose(
        code = paste(result$varName, ".length", sep = ""),
        lobj = NULL)
      lengthResult <- livy_invoke_statement(sc, lengthStatement)

      lapply(seq_len(lengthResult), function(idx) {
        livy_lobj_create(sc, paste(result$varName, "(", (idx - 1), ")", sep = ""))
      })
    }
    else {
      jsonStatement <- livy_statement_compose_magic(result, "json")
      livy_invoke_statement(sc, jsonStatement)
    }
  }
  else {
    result
  }
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
invoke.spark_lobj <- function(jobj, method, ...) {
  statement <- livy_statement_compose_method(jobj, method, ...)
  livy_invoke_statement_fetch(jobj$sc, statement)
}

#' @export
invoke_static.livy_connection <- function(sc, class, method, ...) {
  statement <- livy_statement_compose_static(sc, class, method, ...)
  livy_invoke_statement_fetch(sc, statement)
}

#' @export
invoke_new.livy_connection <- function(sc, class, ...) {
  statement <- livy_statement_compose_new(sc, class, ...)
  livy_invoke_statement_fetch(sc, statement)
}

#' @export
initialize_connection.livy_connection <- function(sc) {
  tryCatch({
    sc$spark_context <- invoke_static(
      sc,
      "org.apache.spark.SparkContext",
      "getOrCreate"
    )

    sc$java_context <- invoke_static(
      sc,
      "org.apache.spark.api.java.JavaSparkContext",
      "fromSparkContext",
      sc$spark_context
    )

    if (spark_version(sc) >= "2.0.0") {
      sc$hive_context <- create_hive_context_v2(sc)
    }
    else {
      sc$hive_context <- invoke_new(
        sc,
        "org.apache.spark.sql.hive.HiveContext",
        sc$spark_context
      )

      # apply configuration
      params <- connection_config(sc, "spark.sql.")
      apply_config(params, hive_context, "setConf", "spark.sql.")
    }

    sc
  }, error = function(err) {
    stop("Failed to initialize livy connection: ", err$message)
  })
}

#' @export
spark_connection.spark_lobj <- function(x, ...) {
  x$sc
}
