# register the spark_connection S3 class for use in setClass slots
methods::setOldClass("spark_connection")
methods::setOldClass("spark_jobj")

.spark_default_version <- numeric_version("1.6.2")

spark_version_numeric <- function(version) {
  numeric_version(gsub("[-_a-zA-Z]", "", version))
}

spark_default_app_jar <- function(version) {
  version <- version %||% .spark_default_version
  sparklyr_jar_path(spark_version_numeric(version))
}

#' Connect to Spark
#'
#' @param master Spark cluster url to connect to. Use \code{"local"} to connect to a local
#'   instance of Spark installed via \code{\link{spark_install}}.
#' @param spark_home Spark home directory (defaults to SPARK_HOME environment variable).
#'   If \code{SPARK_HOME} is defined it will be always be used unless the \code{version}
#'   paramater is specified to force the use of a locally installed version.
#' @param method of connecting to spark (currently only "shell" is supported, additional
#'   methods may come later)
#' @param app_name Application name to be used while running in the Spark cluster
#' @param version Version of Spark (only applicable for local master)
#' @param hadoop_version Version of Hadoop (only applicable for local master)
#' @param extensions Extension packages to enable for this connection. By default will
#'   enable all packages that previously called \code{sparklyr::register_extension}.
#' @param config Configuration for connection (see \code{\link{spark_config} for details}).
#'
#' @return Connection to Spark local instance or remote cluster
#'
#' @family Spark connections
#'
#' @export
spark_connect <- function(master,
                          spark_home = Sys.getenv("SPARK_HOME"),
                          method = c("shell"),
                          app_name = "sparklyr",
                          version = NULL,
                          hadoop_version = NULL,
                          config = spark_config(),
                          extensions = sparklyr::registered_extensions()) {

  # validate method
  method <- match.arg(method)

  # master can be missing if it's specified in the config file
  if (missing(master)) {
    master <- config$spark.master
    if (is.null(master))
      stop("You must either pass a value for master or include a spark.master ",
           "entry in your config.yml")
  }

  # determine whether we need cores in master
  passedMaster <- master
  cores <- config[["sparklyr.cores.local"]]
  if (master == "local" && !identical(cores, NULL))
    master <- paste("local[", cores, "]", sep = "")

  # look for existing connection with the same method, master, and app_name
  filter <- function(e) {
    connection_is_open(e) &&
      identical(e$method, method) &&
      identical(e$master, master) &&
      identical(e$app_name, app_name)
  }
  sconFound <- spark_connection_find_scon(filter)
  if (length(sconFound) == 1) {
    message("Re-using existing Spark connection to ", passedMaster)
    return(sconFound[[1]])
  }

  # determine shell_args (use fake connection b/c we don't yet
  # have a real connection)
  config_sc <- list(config = config, master = master)
  shell_args <- connection_config(config_sc, "sparklyr.shell.")

  # flatten shell_args to make them compatible with sparklyr
  shell_args <- unlist(lapply(names(shell_args), function(name) {
    lapply(shell_args[[name]], function(value) {
      list(paste0("--", name), value)
    })
  }))

  # connect using the specified method

  # spark-shell (local install of spark)
  if (method == "shell") {
    scon <- shell_connection(master = master,
                             spark_home = spark_home,
                             app_name = app_name,
                             version = version,
                             hadoop_version = hadoop_version,
                             shell_args = shell_args,
                             config = config,
                             extensions = extensions)

  # other methods
  } else {

    # e.g.
    # scon <- livy_connection(master = master,
    #                         app_name = app_name,
    #                         shell_args = shell_args,
    #                         config = config,
    #                         extensions = extensions)

    stop("Unsupported connection method '", method, "'")
  }

  # mark the connection as a DBIConnection class to allow DBI to use defaults
  attr(scon, "class") <- c(attr(scon, "class"), "DBIConnection")

  # update spark_context and hive_context connections with DBIConnection
  scon$spark_context$connection <- scon
  scon$hive_context$connection <- scon

  # notify connection viewer of connection
  libs <- c("sparklyr", extensions)
  libs <- vapply(libs,
                 function(lib) paste0("library(", lib, ")"),
                 character("1"),
                 USE.NAMES = FALSE)
  libs <- paste(libs, collapse = "\n")
  if ("package:dplyr" %in% search())
    libs <- paste(libs, "library(dplyr)", sep = "\n")
  parentCall <- match.call()
  connectCall <- paste(libs,
                       paste("sc <-", deparse(parentCall, width.cutoff = 500), collapse = " "),
                       sep = "\n")
  on_connection_opened(scon, connectCall)

  # Register a finalizer to sleep on R exit to support older versions of the RStudio ide
  reg.finalizer(as.environment("package:sparklyr"), function(x) {
    if (connection_is_open(scon)) {
      Sys.sleep(1)
    }
  }, onexit = TRUE)

  # add to our internal list
  spark_connections_add(scon)

  # return scon
  scon
}

#' @export
spark_log.spark_connection <- function(sc, n = 100, ...) {
  if (.Platform$OS.type == "windows") {
    log <- file("log4j.spark.log")
    lines <- readr::read_lines(log)

    tryCatch(function() {
      close(log)
    })

    if (!is.null(n))
      linesLog <- utils::tail(lines, n = n)
    else
      linesLog <- lines
    attr(linesLog, "class") <- "sparklyr_log"

    linesLog
  }
  else {
    class(sc) <- "spark_shell_connection"
    spark_log(sc, n, ...)
  }
}

#' @export
print.sparklyr_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' Check if a Spark connection is open
#'
#' @param sc \code{spark_connection}
#'
#' @rdname spark_connection_is_open
#'
#' @keywords internal
#'
#' @export
spark_connection_is_open <- function(sc) {
  connection_is_open(sc)
}

#' Disconnect from Spark
#'
#' @param x A \code{\link{spark_connect}} connection or the Spark master
#' @param ... Additional arguments
#'
#' @family Spark connections
#'
#' @export
spark_disconnect <- function(x, ...) {
  UseMethod("spark_disconnect")
}

#' @export
spark_disconnect.spark_connection <- function(x, ...) {
  tryCatch({
    stop_shell(x)
  }, error = function(err) {
  })

  spark_connections_remove(x)

  on_connection_closed(x)
}

#' @export
spark_disconnect.character <- function(x, ...) {
  args <- list(...)
  master <- if (!is.null(args$master)) args$master else x
  app_name <- args$app_name

  connections <- spark_connection_find_scon(function(e) {
    e$master <- gsub("\\[\\d*\\]", "", e$master)

    e$master == master &&
      (is.null(app_name) || e$app_name == app_name)
  })

  length(lapply(connections, function(sc) {
    spark_disconnect(sc)
  }))
}

# Get the path to a temp file containing the current spark log (used by IDE)
spark_log_file <- function(sc) {
  scon <- sc
  if (!connection_is_open(scon)) {
    stop("The Spark conneciton is not open anymmore, log is not available")
  }

  lines <- spark_log(sc, n = NULL)
  tempLog <- tempfile(pattern = "spark", fileext = ".log")
  writeLines(lines, tempLog)

  tempLog
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

#' Close all existing connections
#'
#' @family Spark connections
#'
#' @rdname spark_disconnect
#' @export
spark_disconnect_all <- function() {
  scons <- spark_connection_find_scon(function(e) {
    connection_is_open(e)
  })

  length(lapply(scons, function(e) {
    spark_disconnect(e)
  }))
}

spark_inspect <- function(jobj) {
  print(jobj)
  if (!connection_is_open(spark_connection(jobj)))
    return(jobj)

  class <- invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- invoke(class, "getDeclaredFields")
  lapply(fields, function(field) { print(field) })

  cat("Methods:\n")
  methods <- invoke(class, "getDeclaredMethods")
  lapply(methods, function(method) { print(method) })

  jobj
}

