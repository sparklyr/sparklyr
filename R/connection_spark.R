# register the spark_connection S3 class for use in setClass slots

#' spark_connection class
#'
#' @name spark_connection-class
#' @exportClass spark_connection
methods::setOldClass("spark_connection")

#' spark_jobj class
#'
#' @name spark_jobj-class
#' @exportClass spark_jobj
methods::setOldClass("spark_jobj")

.spark_default_version <- numeric_version("1.6.2")

spark_version_numeric <- function(version) {
  numeric_version(gsub("[-_a-zA-Z]", "", version))
}

spark_default_app_jar <- function(version) {
  version <- version %||% .spark_default_version
  sparklyr_jar_path(spark_version_numeric(version))
}

#' Manage Spark Connections
#'
#' These routines allow you to manage your connections to Spark.
#'
#' @param sc A \code{spark_connection}.
#' @param master Spark cluster url to connect to. Use \code{"local"} to
#'   connect to a local instance of Spark installed via
#'   \code{\link{spark_install}}.
#' @param spark_home The path to a Spark installation. Defaults to the path
#'   provided by the \code{SPARK_HOME} environment variable. If
#'   \code{SPARK_HOME} is defined, it will be always be used unless the
#'   \code{version} parameter is specified to force the use of a locally
#'   installed version.
#' @param method The method used to connect to Spark. Currently, only
#'   \code{"shell"} is supported.
#' @param app_name The application name to be used while running in the Spark
#'   cluster.
#' @param version The version of Spark to use. Only applicable to
#'   \code{"local"} Spark connections.
#' @param hadoop_version The version of Hadoop to use. Only applicable to
#'   \code{"local"} Spark connections.
#' @param extensions Extension packages to enable for this connection. By
#'   default, all packages enabled through the use of
#'   \code{\link[=register_extension]{sparklyr::register_extension}} will be passed here.
#' @param config Custom configuration for the generated Spark connection. See
#'   \code{\link{spark_config}} for details.
#' @param ... Optional arguments; currently unused.
#'
#' @name spark-connections
NULL

#' @name spark-connections
#'
#' @examples
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' connection_is_open(sc)
#'
#' spark_disconnect(sc)
#'
#' @export
spark_connect <- function(master,
                          spark_home = Sys.getenv("SPARK_HOME"),
                          method = c("shell", "livy", "databricks", "test"),
                          app_name = "sparklyr",
                          version = NULL,
                          hadoop_version = NULL,
                          config = spark_config(),
                          extensions = sparklyr::registered_extensions(),
                          ...)
{
  # validate method
  method <- match.arg(method)

  # master can be missing if it's specified in the config file
  if (missing(master)) {
    if (identical(method, "databricks")) {
      master <- "databricks"
    } else {
      master <- config$spark.master
      if (is.null(master))
        stop("You must either pass a value for master or include a spark.master ",
             "entry in your config.yml")
    }
  }

  # determine whether we need cores in master
  passedMaster <- master
  cores <- spark_config_value(config, "sparklyr.cores.local")
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

  # clean spark_apply per-connection cache
  if (dir.exists(spark_apply_bundle_path()))
    unlink(spark_apply_bundle_path(), recursive = TRUE)

  # connect using the specified method

  # if master is an example code, run in test mode
  if (master == "spark://HOST:PORT")
    method <- "test"

  if (spark_master_is_gateway(master))
    method <- "gateway"

  # spark-shell (local install of spark)
  if (method == "shell") {
    scon <- shell_connection(master = master,
                             spark_home = spark_home,
                             app_name = app_name,
                             version = version,
                             hadoop_version = hadoop_version,
                             shell_args = shell_args,
                             config = config,
                             service = spark_config_value(
                               config,
                               "sparklyr.gateway.service",
                               FALSE),
                             remote = spark_config_value(
                               config,
                               "sparklyr.gateway.remote",
                               spark_master_is_yarn_cluster(master, config)),
                             extensions = extensions)
  } else if (method == "livy") {
    scon <- livy_connection(master = master,
                            config = config,
                            app_name,
                            version,
                            hadoop_version ,
                            extensions)
  } else if (method == "gateway") {
    scon <- gateway_connection(master = master, config = config)
  } else if (method == "databricks") {
    scon <- databricks_connection(config = config,
                                  extensions)
  } else if (method == "test") {
    scon <- test_connection(master = master,
                            config = config,
                            app_name,
                            version,
                            hadoop_version ,
                            extensions)
  } else {
    # other methods

    stop("Unsupported connection method '", method, "'")
  }

  scon <- initialize_connection(scon)

  # mark the connection as a DBIConnection class to allow DBI to use defaults
  attr(scon, "class") <- c(attr(scon, "class"), "DBIConnection")

  # update spark_context and hive_context connections with DBIConnection
  scon$spark_context$connection <- scon

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

  # let viewer know that we've opened a connection; guess that the result will
  # be assigned into the global environment
  on_connection_opened(scon, globalenv(), connectCall)

  # Register a finalizer to sleep on R exit to support older versions of the RStudio IDE
  reg.finalizer(asNamespace("sparklyr"), function(x) {
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
spark_log.spark_connection <- function(sc, n = 100, filter = NULL, ...) {
  if (.Platform$OS.type == "windows") {
    log <- file("logs/log4j.spark.log")
    lines <- readr::read_lines(log)

    tryCatch(function() {
      close(log)
    })

    if (!is.null(filter)) {
      lines <- lines[grepl(filter, lines)]
    }

    if (!is.null(n))
      linesLog <- utils::tail(lines, n = n)
    else
      linesLog <- lines

    attr(linesLog, "class") <- "sparklyr_log"

    linesLog
  }
  else {
    spark_log.spark_shell_connection(sc, n = n, filter, ...)
  }
}

#' @export
print.sparklyr_log <- function(x, ...) {
  cat(x, sep = "\n")
  cat("\n")
}

#' @name spark-connections
#' @export
spark_connection_is_open <- function(sc) {
  connection_is_open(sc)
}

#' @name spark-connections
#' @export
spark_disconnect <- function(sc, ...) {
  UseMethod("spark_disconnect")
}

#' @export
spark_disconnect.spark_connection <- function(sc, ...) {
  tryCatch({
    subclass <- remove_class(sc, "spark_connection")
    spark_disconnect(subclass, ...)
  }, error = function(err) {
  })

  spark_connections_remove(sc)

  on_connection_closed(sc)
}

#' @export
spark_disconnect.character <- function(sc, ...) {
  args <- list(...)
  master <- if (!is.null(args$master)) args$master else sc
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

spark_connection_is_yarn_client <- function(sc) {
  spark_master_is_yarn_client(sc$master)
}

spark_master_is_yarn_client <- function(master) {
  grepl("^yarn-client$", master, ignore.case = TRUE, perl = TRUE)
}

spark_master_is_yarn_cluster <- function(master, config) {
  grepl("^yarn-cluster$", master, ignore.case = TRUE, perl = TRUE) ||
    (
      identical(config[["sparklyr.shell.deploy-mode"]], "cluster") &&
      identical(master, "yarn")
    )
}

spark_master_is_gateway <- function(master) {
  grepl("sparklyr://.*", master)
}

# Number of cores available in the local install
spark_connection_local_cores <- function(sc) {
  spark_config_value(sc$config, "sparklyr.cores")
}

#' @name spark-connections
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

