#' Start the Spark R Shell
#'
#' @param master Spark cluster url to connect to. Use \code{"local"} to connect to a local
#'   instance of Spark
#' @param spark_home Spark home directory (defaults to SPARK_HOME environment variable)
#' @param spark_version Spark version, if not specified, version taken from SPARK_HOME
#' @param app_name Application name to be used while running in the Spark cluster
#' @param config Named character vector of spark.* options
#' @param jars Paths to Jar files to include
#' @param packages Spark packages to include
#' @param extensions Extension packages to include dependencies for
#'   (see \code{\link{spark_dependency}}).
#' @param environment Environment variables to set
#' @param shell_args Additional command line arguments for spark_shell
#' @param sc \code{spark_connection}
#'
#' @return \code{spark_connection} object
#'
#' @export
start_shell <- function(master,
                        spark_home = Sys.getenv("SPARK_HOME"),
                        spark_version = NULL,
                        app_name = "sparkapi",
                        config = list(),
                        extensions = sparkapi::registered_extensions(),
                        jars = NULL,
                        packages = NULL,
                        environment = NULL,
                        shell_args = NULL) {
  # read app jar through config, this allows "sparkr-shell" to test sparkr backend
  app_jar <- spark_config_value(config, "sparkapi.app.jar", NULL)
  if (is.null(app_jar)) {
    app_jar <- shQuote(normalizePath(system.file(file.path("java", "sparkapi-1.6.1.jar"), package = "sparkapi"),
                             mustWork = FALSE))
    shell_args <- c(shell_args, "--class", "sparkapi.Backend")
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
    windows = "spark-submit.cmd"
  )
  spark_submit_path <- normalizePath(file.path(spark_home, "bin", spark_submit))

  # resolve extensions
  spark_version <- numeric_version(
    ifelse(is.null(spark_version),
      spark_version_from_home(spark_home),
      gsub("[-_a-zA-Z]", "", spark_version)
    )
  )
  scala_version <- numeric_version("2.10")
  extensions <- spark_dependencies_from_extensions(spark_version, scala_version, extensions)

  # combine passed jars and packages with extensions
  jars <- normalizePath(unique(c(jars, extensions$jars)))
  packages <- unique(c(packages, extensions$packages))

  # add jars to arguments
  if (length(jars) > 0) {
    shell_args <- c(shell_args, "--jars", paste(shQuote(jars), collapse=","))
  }

  # add packages to arguments
  if (length(packages) > 0) {
    shell_args <- c(shell_args, "--packages", paste(shQuote(packages), collapse=","))
  }

  # add sparkr-shell to args
  shell_args <- c(shell_args, app_jar)

  # create temporary file for shell ports output and add it to the args
  shell_output_path <- spark_config_value(config,
                                          "sparkapi.ports.file",
                                          normalizePath(tempfile(fileext = ".out"),
                                                        mustWork = FALSE))

  on.exit(unlink(shell_output_path))
  shell_args <- c(shell_args, shell_output_path)

  # create temp file for stdout and stderr
  output_file <- tempfile(fileext = "_spark.log")
  error_file <- tempfile(fileext = "_spark.err")

  # start the shell (w/ specified additional environment variables)
  env <- unlist(environment)
  withr::with_envvar(env, {
    if (.Platform$OS.type == "windows") {
      shell(paste(
        spark_submit_path,
        paste(shell_args, collapse = " "),
        ">",
        output_file,
        "2>",
        error_file
      ),
      wait = FALSE)
    }
    else {
      system2(spark_submit_path,
              args = shell_args,
              stdout = output_file,
              stderr = output_file,
              wait = FALSE)
    }
  })

  # wait for the shell output file
  waitSeconds <- spark_config_value(config, "sparkapi.ports.wait.seconds", 100)
  if (!wait_file_exists(shell_output_path, waitSeconds)) {
    stop(paste(
      "Failed to launch Spark shell. Ports file does not exist.\n",
      "    Path: ", spark_submit_path, "\n",
      "    Parameters: ", paste(shell_args, collapse = ", "), "\n",
      "    \n",
      paste(readLines(output_file), collapse = "\n"),
      if (file.exists(error_file)) paste(readLines(error_file), collapse = "\n") else "",
      sep = ""))
  }

  # read the shell output file
  shell_file <- read_shell_file(shell_output_path)

  # bind to the monitor and backend ports
  tryCatch({
    monitor <- socketConnection(port = shell_file$monitorPort)
  }, error = function(err) {
    stop("Failed to open connection to monitor")
  })

  tryCatch({
    backend <- socketConnection(host = "localhost",
                                port = shell_file$backendPort,
                                server = FALSE,
                                blocking = TRUE,
                                open = "wb",
                                timeout = 6000)
  }, error = function(err) {
    stop("Failed to open connection to backend")
  })

  # create the shell connection
  sc <- structure(class = c("spark_connection", "spark_shell_connection"), list(
    # spark_connection
    master = master,
    spark_home = spark_home,
    app_name = app_name,
    config = config,
    # spark_shell_connection
    backend = backend,
    monitor = monitor,
    output_file = output_file
  ))

  # stop shell on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc)
    }
  }, onexit = TRUE)

  # initialize and return the connection
  initialize_connection(sc)
}


#' Stop the Spark R Shell
#'
#' @rdname start_shell
#'
#' @export
stop_shell <- function(sc) {
  invoke_method(sc,
                FALSE,
                "Handler",
                "stopBackend")

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
spark_log.spark_shell_connection <- function(sc, n = 100, ...) {
  log <- file(sc$output_file)
  lines <- readLines(log)
  close(log)

  if (!is.null(n))
    linesLog <- utils::tail(lines, n = n)
  else
    linesLog <- lines
  attr(linesLog, "class") <- "spark_log"

  linesLog
}

#' @export
spark_web.spark_shell_connection <- function(sc, ...) {
  lines <- spark_log(sc, n = 200)

  uiLine <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexpr("http://.*", uiLine, perl=TRUE)
    match <-regmatches(uiLine, matches)
    if (length(match) > 0) {
      return(structure(match, class = "spark_web_url"))
    }
  }

  uiLine <- grep(".*Bound SparkUI to.*", lines, perl=TRUE, value=TRUE)
  if (length(uiLine) > 0) {
    matches <- regexec(".*Bound SparkUI to.*and started at (http.*)", uiLine, perl=TRUE)
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
  writeBoolean(rc, static)
  writeString(rc, object)
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
    if (nzchar(msg))
      stop(msg, call. = FALSE)
    else {
      # read the spark log
      msg <- read_spark_log_error(sc)
      stop(msg, call. = FALSE)
    }
  }

  object <- readObject(backend)
  attach_connection(object, sc)
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


read_shell_file <- function(shell_file) {

  shellOutputFile <- file(shell_file, open = "rb")
  backendPort <- readInt(shellOutputFile)
  monitorPort <- readInt(shellOutputFile)
  rLibraryPath <- readString(shellOutputFile)
  close(shellOutputFile)

  success <- length(backendPort) > 0 && backendPort > 0 &&
    length(monitorPort) > 0 && monitorPort > 0 &&
    length(rLibraryPath) == 1

  if (!success)
    stop("Invalid values found in shell output")

  list(
    backendPort = backendPort,
    monitorPort = monitorPort,
    rLibraryPath = rLibraryPath
  )
}


wait_file_exists <- function(filename, seconds) {
  retries <- seconds * 10
  while(!file.exists(filename) && retries >= 0) {
    retries <- retries  - 1;
    Sys.sleep(0.1)
  }

  file.exists(filename)
}

read_spark_log_error <- function(sc) {
  # if there was no error message reported, then
  # return information from the Spark logs. return
  # all those with most recent timestamp
  msg <- "failed to invoke spark command (unknown reason)"
  try(silent = TRUE, {
    log <- sc$output_file
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
  if(is.null(config[[name]])) default else config[[name]]
}
