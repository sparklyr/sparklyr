

#' @import sparkapi
NULL

# register the spark_connection S3 class for use in setClass slots
methods::setOldClass("sparklyr_connection")

spark_default_jars <- function() {
  jarsOption <- getOption("spark.jars.default", NULL)

  if (is.null(jarsOption))
    system.file(file.path("java", "rspark_utils.jar"), package = "sparklyr")
  else
    jarsOption
}

#' Connect to Spark
#' 
#' @param master Spark cluster url to connect to. Use \code{"local"} to connect to a local 
#'   instance of Spark installed via \code{\link{spark_install}}.
#' @param app_name Application name to be used while running in the Spark cluster
#' @param version Version of the Spark (only applicable for local master)
#' @param hadoop_version Version of Hadoop (only applicable for local master)
#' @param extensions Extension packages to enable for this connection.
#' @param config Configuration for connection (see \code{\link{spark_config} for details}).
#' 
#' @return Connection to Spark local instance or remote cluster
#' 
#' @family Spark connections
#' 
#' @export
spark_connect <- function(master,
                          app_name = "sparklyr",
                          version = NULL,
                          hadoop_version = NULL,
                          extensions = NULL,
                          config = spark_config()) {
  
  # master can be missing if it's specified in the config file
  if (missing(master)) {
    master <- config$spark.master
    if (is.null(master))
      stop("You must either pass a value for master or include a spark.master ",
           "entry in your config.yml")
  }
  
  filter <- function(e) {
    spark_connection_is_open(e) &&
    identical(e$master, master)
  }
  
  sconFound <- spark_connection_find_scon(filter)
  if (length(sconFound) == 1) {
    return(sconFound[[1]])
  }

  # verify that java is available
  if (!is_java_available()) {
    stop("Java is required to connect to Spark. Please download and install Java from ",
         java_install_url())
  }
  
  # attach unknown error handler
  sparkapi_unknown_error_handler(read_spark_log_error)

  sparkHome <- spark_home()
  if (spark_master_is_local(master)) {
    if (is.null(sparkHome) || !is.null(version) || !is.null(hadoop_version)) {
      installInfo <- spark_install_find(version, hadoop_version, latest = FALSE)
      sparkHome <- installInfo$sparkVersionDir
    }
  } else {
    if (is.null(sparkHome)) {
      stop("Failed to launch in cluster mode, SPARK_HOME environment variable is not set.")
    }
  }
  
  scon <- list(
    master = master,
    appName = app_name,
    sparkHome = sparkHome,
    config = config
  )
  scon <- structure(scon, class = c("sparklyr_connection", "sparkapi_connection"))

  # determine jars and packages
  jars <- spark_default_jars()
  packages <- config[["sparklyr.defaultPackages"]]
  
  # call extensions to get additional jars and packages
  dependencies <- resolve_extensions(extensions, config)
  jars <- c(jars, dependencies$jars)
  packages <- c(packages, dependencies$packages)
  
  sconInst <- start_shell(scon, list(), jars, packages)
  scon$backend = sconInst$backend
  scon$monitor = sconInst$monitor
  
  scon <- spark_connection_add_inst(scon, sconInst)

  # start with library(sparklyr)
  libs <- "library(sparklyr)"
  
  # check for dplyr on search path
  if ("package:dplyr" %in% search())
    libs <- paste(libs, "library(dplyr)", sep = "\n")
  
  parentCall <- match.call()
  sconInst$connectCall <- paste(libs,
                                paste("sc <-", deparse(parentCall, width.cutoff = 500), collapse = " "),
                                sep = "\n")
  sconInst$onReconnect = list()

  reg.finalizer(baseenv(), function(x) {
    if (spark_connection_is_open(scon)) {
      stop_shell(scon)
    }
  }, onexit = TRUE)

  sconInst <- spark_connection_attach_context(scon, sconInst)
  scon$spark_context <- sconInst$sc
  spark_connection_set_inst(scon, sconInst)

  sconInst <- spark_connection_attach_sql_session_context(scon, sconInst)
  scon$hive_context <- sconInst$hive
  spark_connection_set_inst(scon, sconInst)
  
  # notify listeners
  on_connection_opened(scon, sconInst$connectCall)
  
  # return scon
  scon
}

resolve_extensions <- function(extensions, config) {
  sparkapi_dependencies_from_extensions(config, extensions)
}

# Attaches the SparkContext to the connection
spark_connection_attach_context <- function(sc, sconInst) {
  scon <- sc
  master <- scon$master

  cores <- scon$config[["sparklyr.cores"]]
  if (scon$master == "local" && !identical(cores, NULL))
    master <- paste("local[", cores, "]", sep = "")

  sconInst$sc <- spark_connection_create_context(scon, master, scon$appName, scon$sparkHome)
  if (identical(sconInst$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  sconInst
}

# Attaches the SqlContext/SessionContext to the connection
spark_connection_attach_sql_session_context <- function(sc, sconInst) {
  scon <- sc

  if (is.null(sconInst$hive)) {
    sconInst$hive <- spark_api_create_hive_context(scon)
    if (identical(sconInst$hive, NULL)) {
      warning("Failed to create Hive context, falling back to SQL. Some operations, like window-funcitons, will not work")
    }
  }

  if (is.null(sconInst$hive)) {
    sconInst$sql <- spark_api_create_sql_context(scon)
    if (identical(sql, NULL)) {
      stop("Failed to create SQL context")
    }
  }

  sconInst
}

#' Disconnect from Spark
#' 
#' @param sc Spark connection provided by \code{\link{spark_connect}}
#' 
#' @family Spark connections
#' 
#' @export
spark_disconnect <- function(sc) {
  stop_shell(sc)
}

#' Retrieves entries from the Spark log
#' 
#' @inheritParams spark_disconnect
#' @param n Max number of log entries to retrieve
#' 
#' @return Character vector with last \code{n} lines of the Spark log 
#'   or for \code{spark_log_file} the full path to the log file.
#' 
#' @family Spark connections
#' 
#' @export
spark_log <- function(sc, n = 100) {
  scon <- sc
  log <- file(spark_log_file(scon))
  lines <- readLines(log)
  close(log)

  linesLog <- tail(lines, n = n)
  attr(linesLog, "class") <- "spark_log"

  linesLog
}

#' @rdname spark_log
#' @export
spark_log_file <- function(sc) {
  scon <- sc
  if (!spark_connection_is_open(scon)) {
    stop("The Spark conneciton is not open anymmore, log is not available")
  }
  sconInst <- spark_connection_get_inst(scon)
  sconInst$outputFile
}

#' @export
print.spark_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' Open the Spark web interface
#' 
#' @inheritParams spark_disconnect
#' 
#' @family Spark connections
#' 
#' @export
spark_web <- function(sc) {
  scon <- sc
  sconInst <- spark_connection_get_inst(scon)
  log <- file(sconInst$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  foundMatch <- FALSE
  uiLine <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine, perl=TRUE)
    match <-regmatches(uiLine, matches)
    if (length(match) > 0) {
      return(structure(match, class = "spark_web_url"))
    }
  }

  warning("Spark UI URL not found in logs, attempting to guess.")
  structure("http://localhost:4040", class = "spark_web_url")
}

#' @export
print.spark_web_url <- function(x, ...) {
  utils::browseURL(x)
}

read_spark_log_error <- function(sc) {
  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- spark_log(sc)
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


# API into https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
#
# def createSparkContext(
#   master: String,                               // The Spark master URL.
#   appName: String,                              // Application name to register with cluster manager
#   sparkHome: String,                            // Spark Home directory
#   jars: Array[String],                          // Character string vector of jar files to pass to the worker nodes.
#   sparkEnvirMap: JMap[Object, Object],          // Named list of environment variables to set on worker nodes.
#   sparkExecutorEnvMap: JMap[Object, Object])    // Named list of environment variables to be used when launching executors.
#   : JavaSparkContext
#
spark_connection_create_context <- function(sc, master, appName, sparkHome) {
  scon <- sc
  sparkHome <- as.character(normalizePath(sparkHome, mustWork = FALSE))

  conf <- sparkapi_invoke_new(scon, "org.apache.spark.SparkConf")
  conf <- sparkapi_invoke(conf, "setAppName", appName)
  conf <- sparkapi_invoke(conf, "setMaster", master)
  conf <- sparkapi_invoke(conf, "setSparkHome", sparkHome)

  params <- spark_config_params(scon$config, spark_connection_is_local(scon), "spark.context.")
  lapply(names(params), function(paramName) {
    conf <<- sparkapi_invoke(conf, "set", paramName, params[[paramName]])
  })

  sparkapi_invoke_new(
    scon,
    "org.apache.spark.SparkContext",
    conf
  )
}


# Retrieves master from a Spark Connection
spark_connection_master <- function(sc) {
  sc$master
}

# Retrieves the application name from a Spark Connection
spark_connection_app_name <- function(sc) {
  sc$appName
}

# TRUE if the Spark Connection is a local install
spark_connection_is_local <- function(sc) {
  spark_master_is_local(sc$master)
}

spark_master_is_local <- function(master) {
  grepl("^local(\\[[0-9\\*]*\\])?$", master, perl = TRUE)
}

# Number of cores available in the local install
spark_connection_local_cores <- function(sc) {
  sc$config[["sparklyr.cores"]]
}

#' Check to see if the connection to Spark is still open
#' 
#' @inheritParams spark_disconnect
#' 
#' @family Spark connections
#' 
#' @keywords internal
#' 
#' @export
spark_connection_is_open <- function(sc) {
  sconInst <- spark_connection_get_inst(sc)

  bothOpen <- FALSE
  if (!identical(sconInst, NULL)) {
    backend <- sconInst$backend
    monitor <- sconInst$monitor

    tryCatch({
      bothOpen <- isOpen(backend) && isOpen(monitor)
    }, error = function(e) {
    })
  }

  bothOpen
}

spark_connection_version <- function(sc) {
  sparkapi_invoke(sparkapi_spark_context(sc), "version")
}

#' Close all existing connections
#' 
#' @family Spark connections
#' 
#' @rdname spark_disconnect
#' @export
spark_disconnect_all <- function() {
  scons <- spark_connection_find_scon(function(e) {
    spark_connection_is_open(e)
  })

  length(lapply(scons, function(e) {
    spark_disconnect(e)
  }))
}
