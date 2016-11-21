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
      if (is.character(param)) {
        # substitute illegal characters
        param <- gsub("\n", "\" + sys.props(\"line.separator\") + \"", param)

        paste("\"", param, "\"", sep = "")
      }
      else if ("livy_jobj" %in% paramClass) {
        paste(param$varName, "._2", sep = "")
      }
      else if (is.numeric(param)) {
        paste(
          "new java.lang.Integer(",
          param,
          ")",
          sep = ""
        )
      }
      else if (is.logical(param)) {
        paste(
          "new java.lang.Boolean(",
          if (isTRUE(param)) "true" else "false",
          ")",
          sep = ""
        )
      }
      else if (is.list(param)) {
        paste(
          "Array(",
          livy_code_quote_parameters(param),
          ")",
          sep = ""
        )
      }
      else {
        stop("Unsupported parameter ", param, " of class ", paste(paramClass, collapse = ", "), " detected")
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

livy_statement_compose <- function(sc, static, class, method, ...) {
  serialized <- livy_invoke_serialize(sc = sc, static = static, object = class, method = method, ...)

  varName <- livy_code_new_return_var(sc)

  code <- paste(
    "var ", varName, " = ",
    "LivyUtils.invokeFromBase64(\"",
    serialized,
    "\")",
    sep = ""
  )

  livy_statement_new(
    code = code,
    lobj = livy_lobj_create(sc, varName)
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

livy_statement_parse_response <- function(text, lobj) {
  nullResponses <- list(
    "defined module.*"
  )

  if (regexec(paste(nullResponses, collapse = "|"), text)[[1]][[1]] > 0) {
    return(NULL)
  }

  text <- gsub("\n", "", text)

  parsedRegExp <- regexec("([^:]+): (.*) = (.*)", text, perl = TRUE)
  parsed <- regmatches(text, parsedRegExp)
  if (length(parsed) != 1) {
    stop("Failed to parse statement reponse: ", text)
  }

  parsed <- parsed[[1]]
  if (length(parsed) != 4) {
    stop("Failed to parse statement reponse: ", text)
  }

  varName <- parsed[[2]]
  scalaTypeRaw <- parsed[[3]]
  scalaValue <- parsed[[4]]

  removeQuotes <- function(e) gsub("^\"|\"$", "", e)

  livyToRTypeMap <- list(
    "String" = list(
      type = "character",
      parse = function(e) removeQuotes(e)
    ),
    "java.lang.String" = list(
      type = "character",
      parse = function(e) removeQuotes(e)
    ),
    "java.io.Serializable" = list(
      type = "character",
      parse = function(e) removeQuotes(e)
    ),
    "int" = list(
      type = "integer",
      parse = function(e) as.integer(e)
    ),
    "java.lang.Integer" = list(
      type = "integer",
      parse = function(e) as.integer(e)
    ),
    "null" = list(
      type = "NULL",
      parse = function(e) NULL
    )
  )

  scalaTypeIsArray <- function(scalaType) grepl("^\\[.*", scalaType, perl = TRUE)
  scalaTypeOfArray <- function(scalaType) {
    parsed <- regmatches(scalaType, regexec("^\\[L(.*);", scalaType, perl = TRUE))
    parsed[[1]][[2]]
  }

  scalaType <- if (scalaTypeIsArray(scalaTypeRaw)) scalaTypeOfArray(scalaTypeRaw) else scalaTypeRaw

  type <- "object"
  lobj$varType <- scalaType
  value <- lobj

  if (scalaType %in% names(livyToRTypeMap)) {
    livyToRTypeMapInst <- livyToRTypeMap[[scalaType]]
    type <- livyToRTypeMapInst$type
    value <- livyToRTypeMapInst$parse(scalaValue)
  }

  if (scalaTypeIsArray(scalaTypeRaw)) {
    lobj$collection = list(
      type = "array",
      entries = type
    )
    lobj
  }
  else {
    value
  }
}

livy_get_statement <- function(sc, statementId) {
  statement <- fromJSON(paste(sc$master, "sessions", sc$sessionId, "statements", statementId, sep = "/"))

  assert_that(!is.null(statement$state))
  assert_that(statement$id == statementId)

  statement
}

livy_inspect <- function(lobj) {

}

livy_post_statement <- function(sc, code) {
  write(code, file = sc$log, append = TRUE)

  req <- POST(paste(sc$master, "sessions", sc$sessionId, "statements", sep = "/"),
    add_headers(
      "Content-Type" = "application/json"
    ),
    body = toJSON(
      list(
        code = unbox(code)
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
  sleepTime <- 0.001
  while (statementReponse$state == "running" &&
         waitTimeout > 0) {
    statementReponse <- livy_get_statement(sc, statementReponse$id)

    Sys.sleep(sleepTime)

    waitTimeout <- waitTimeout - 1
    sleepTime <- sleepTime * 2
  }

  if (statementReponse$state != "available") {
    stop("Failed to execute Livy statement with state ", statementReponse$state)
  }

  assert_that(!is.null(statementReponse$output))

  if (statementReponse$output$status == "error") {
    withr::with_options(list(
      warning.length = 8000
    ), {
      stop("Failed to execute Livy statement with error: ", statementReponse$output$evalue)
    })
  }

  data <- statementReponse$output$data

  if ("text/plain" == names(data)[[1]]) {
    write("\n", file = sc$log, append = TRUE)
    write(data[[1]], file = sc$log, append = TRUE)
    write("\n", file = sc$log, append = TRUE)
  }

  data
}

livy_invoke_statement <- function(sc, statement) {
  data <- livy_post_statement(sc, statement$code)
  assert_that(!is.null(data))

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

  if (!is.character(result)) {
    stop("Failed to execute statement, character result expected but ", typeof(result), " was received.")
  }

  lobj <- livy_invoke_deserialize(sc, result)
  lobj
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
    code = new.env(),
    log = tempfile(fileext = ".log")
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
    stop(
      "Failed to launch livy session, session status is",
      " still starting after waiting for ", waitStartTimeout, " seconds")
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

livy_map_class <- function(class) {
  gsub("sparklyr.", "", class)
}

#' @export
invoke.livy_jobj <- function(jobj, method, ...) {
  statement <- livy_statement_compose(spark_connection(jobj), FALSE, jobj, method, ...)
  livy_invoke_statement_fetch(jobj$sc, statement)
}

#' @export
invoke_static.livy_connection <- function(sc, class, method, ...) {
  classMapped <- livy_map_class(class)

  statement <- if (grepl("^sparklyr\\.", class)) {
    lobjInternal <- livy_lobj_create(sc, classMapped)
    livy_statement_compose(lobjInternal, FALSE, method, ...)
  } else {
    livy_statement_compose(sc, TRUE, classMapped, method, ...)
  }

  livy_invoke_statement_fetch(sc, statement)
}

#' @export
invoke_new.livy_connection <- function(sc, class, ...) {
  class <- livy_map_class(class)

  statement <- livy_statement_compose_new(sc, class, ...)
  livy_invoke_statement_fetch(sc, statement)
}

invokeRaw <- function(sc, code, ...) {
  sparklyr:::livy_post_statement(sc, code)
}

livy_load_scala_sources <- function(sc) {
  livySources <- c(
    "utils.scala",
    "sqlutils.scala",
    "logging.scala",
    "invoke.scala",
    "tracker.scala",
    "serializer.scala",
    "stream.scala",
    "livyutils.scala"
  )

  lapply(livySources, function(sourceName) {
    tryCatch({
      sourcesFile <- system.file(file.path("livy", sourceName), package = "sparklyr")
      sources <- paste(readLines(sourcesFile), collapse = "\n")

    statement <- livy_statement_new(sources, NULL)
    livy_invoke_statement(sc, statement)
  }, error = function(e) {
    stop("Failed to load ", sourceName, ": ", e$message)
  })
  })
}

#' @export
initialize_connection.livy_connection <- function(sc) {
  tryCatch({
    livy_load_scala_sources(sc)

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

