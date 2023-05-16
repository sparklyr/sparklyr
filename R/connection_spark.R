# register the spark_connection S3 class for use in setClass slots

#' spark_connection class
#'
#' @name spark_connection-class
#' @exportClass spark_connection
#' @include spark_version.R
NULL

methods::setOldClass(c("livy_connection", "spark_connection"))
methods::setOldClass(c("databricks_connection", "spark_gateway_connection", "spark_shell_connection", "spark_connection"))
methods::setOldClass(c("test_connection", "spark_connection"))

#' spark_jobj class
#'
#' @name spark_jobj-class
#' @exportClass spark_jobj
methods::setOldClass("spark_jobj")

.spark_default_version <- numeric_version("1.6.2")

spark_version_numeric <- function(version) {
  numeric_version(gsub("[-_a-zA-Z]", "", version))
}

spark_default_app_jar <- function(version, scala_version = NULL) {
  version <- version %||% .spark_default_version
  sparklyr_jar_path(spark_version_numeric(version), scala_version)
}

#' Manage Spark Connections
#'
#' These routines allow you to manage your connections to Spark.
#'
#' @param sc A \code{spark_connection}.
#' @param master Spark cluster url to connect to. Use \code{"local"} to
#'   connect to a local instance of Spark installed via
#'   \code{\link[=spark_install]{spark_install}}.
#' @param spark_home The path to a Spark installation. Defaults to the path
#'   provided by the \code{SPARK_HOME} environment variable. If
#'   \code{SPARK_HOME} is defined, it will always be used unless the
#'   \code{version} parameter is specified to force the use of a locally
#'   installed version.
#' @param method The method used to connect to Spark. Default connection method
#'   is \code{"shell"} to connect using spark-submit, use \code{"livy"} to
#'   perform remote connections using HTTP, or \code{"databricks"} when using a
#'   Databricks clusters.
#' @param app_name The application name to be used while running in the Spark
#'   cluster.
#' @param version The version of Spark to use. Required for \code{"local"} Spark
#'   connections, optional otherwise.
#' @param extensions Extension R packages to enable for this connection. By
#'   default, all packages enabled through the use of
#'   \code{\link[=register_extension]{sparklyr::register_extension}} will be passed here.
#' @param config Custom configuration for the generated Spark connection. See
#'   \code{\link{spark_config}} for details.
#' @param packages A list of Spark packages to load. For example, \code{"delta"} or
#'   \code{"kafka"} to enable Delta Lake or Kafka. Also supports full versions like
#'   \code{"io.delta:delta-core_2.11:0.4.0"}. This is similar to adding packages into the
#'   \code{sparklyr.shell.packages} configuration option. Notice that the \code{version}
#'   parameter is used to choose the correct package, otherwise assumes the latest version
#'   is being used.
#' @param scala_version Load the sparklyr jar file that is built with the version of
#'   Scala specified (this currently only makes sense for Spark 2.4, where sparklyr will
#'   by default assume Spark 2.4 on current host is built with Scala 2.11, and therefore
#'   `scala_version = '2.12'` is needed if sparklyr is connecting to Spark 2.4 built with
#'   Scala 2.12)
#'
#' @param ... Optional arguments; currently unused.
#'
#' @name spark-connections
NULL

spark_master_local_cores <- function(master, config) {
  cores <- spark_config_value(config, c("sparklyr.cores.local", "sparklyr.connect.cores.local"))
  if (master == "local" && !identical(cores, NULL)) {
    master <- paste("local[", cores, "]", sep = "")
  }

  master
}

spark_config_shell_args <- function(config, master) {
  if (!is.null(config$sparklyr.shell.packages)) {
    config$sparklyr.shell.packages <- paste0(config$sparklyr.shell.packages, collapse = ",")
  }

  # determine shell_args (use fake connection b/c we don't yet
  # have a real connection)
  config_sc <- list(config = config, master = master)
  shell_args <- connection_config(config_sc, "sparklyr.shell.")

  # flatten shell_args to make them compatible with sparklyr
  unlist(lapply(seq_along(shell_args), function(idx) {
    name <- names(shell_args)[[idx]]
    value <- shell_args[[idx]]

    lapply(as.list(value), function(x) list(paste0("--", name), x))
  }))
}

no_databricks_guid <- function() {
  mget("DATABRICKS_GUID", envir = .GlobalEnv, ifnotfound = "") == ""
}

#' @name spark-connections
#'
#' @examples
#' conf <- spark_config()
#' conf$`sparklyr.shell.conf` <- c(
#'   "spark.executor.extraJavaOptions=-Duser.timezone='UTC'",
#'   "spark.driver.extraJavaOptions=-Duser.timezone='UTC'",
#'   "spark.sql.session.timeZone='UTC'"
#' )
#'
#' sc <- spark_connect(
#'   master = "spark://HOST:PORT", config = conf
#' )
#' connection_is_open(sc)
#'
#' spark_disconnect(sc)
#' @details
#'
#' By default, when using \code{method = "livy"}, jars are downloaded from GitHub. But
#' an alternative path (local to Livy server or on HDFS or HTTP(s)) to \code{sparklyr}
#' JAR can also be specified through the \code{sparklyr.livy.jar} setting.
#'
#' @export
spark_connect <- function(master,
                          spark_home = Sys.getenv("SPARK_HOME"),
                          method = c("shell", "livy", "databricks", "test", "qubole", "synapse"),
                          app_name = "sparklyr",
                          version = NULL,
                          config = spark_config(),
                          extensions = sparklyr::registered_extensions(),
                          packages = NULL,
                          scala_version = NULL,
                          ...) {
  # validate method
  method <- match.arg(method)

  # A Databricks GUID indicates that it is running on a Databricks cluster,
  # so if there is no GUID, then method = "databricks" must refer to Databricks Connect
  if (method == "databricks" && no_databricks_guid()) {
    method <- "databricks-connect"
    master <- "local"
  }
  hadoop_version <- list(...)$hadoop_version

  # ensure app_name is part of the spark-submit args
  config$`sparklyr.shell.name` <- config$`sparklyr.shell.name` %||% app_name
  master_override <- spark_config_value(config, "sparklyr.connect.master", NULL)
  if (!is.null(master_override)) master <- master_override

  # master can be missing if it's specified in the config file
  if (missing(master)) {
    if (identical(method, "databricks")) {
      master <- "databricks"
    } else if (identical(method, "qubole")) {
      master <- "yarn-client"
      spark_home <- "/usr/lib/spark"
    } else {
      master <- spark_config_value(config, "spark.master", NULL)
      if (is.null(master)) {
        stop(
          "You must either pass a value for master or include a spark.master ",
          "entry in your config.yml"
        )
      }
    }
  }

  # add packages to config
  if (!is.null(packages)) {
    config <- spark_config_packages(
      config,
      packages,
      version %||% spark_version_from_home(spark_home),
      scala_version,
      method = method
    )
  }

  if (is.null(spark_home) || !nzchar(spark_home)) spark_home <- spark_config_value(config, "spark.home", "")

  # increase default memory
  if (spark_master_is_local(master) &&
    identical(spark_config_value(config, "sparklyr.shell.driver-memory"), NULL) &&
    java_is_x64()) {
    config$`sparklyr.shell.driver-memory` <- "2g"
  }

  # determine whether we need cores in master
  passedMaster <- master
  master <- spark_master_local_cores(master, config)

  # look for existing connection with the same method, master, and app_name
  sconFound <- spark_connection_find(master, app_name, method)
  if (length(sconFound) == 1) {
    message("Re-using existing Spark connection to ", passedMaster)
    return(sconFound[[1]])
  }

  shell_args <- spark_config_shell_args(config, master)

  # clean spark_apply per-connection cache
  if (dir.exists(spark_apply_bundle_path())) {
    unlink(spark_apply_bundle_path(), recursive = TRUE)
  }

  # connect using the specified method

  # if master is an example code, run in test mode
  if (master == "spark://HOST:PORT") {
    method <- "test"
  }

  if (spark_master_is_gateway(master)) {
    method <- "gateway"
  }

  # spark-shell (local install of spark)
  if (method == "shell" || method == "qubole" || method == "databricks-connect") {
    scon <- shell_connection(
      master = master,
      spark_home = spark_home,
      method = method,
      app_name = app_name,
      version = version,
      hadoop_version = hadoop_version,
      shell_args = shell_args,
      config = config,
      service = spark_config_value(
        config,
        "sparklyr.gateway.service",
        FALSE
      ),
      remote = spark_config_value(
        config,
        "sparklyr.gateway.remote",
        spark_master_is_yarn_cluster(master, config)
      ),
      extensions = extensions,
      batch = NULL,
      scala_version = scala_version
    )
    if (method != "shell") {
      scon$method <- method
    }
  } else if (method == "livy") {
    scon <- livy_connection(master,
      config,
      app_name,
      version,
      hadoop_version,
      extensions,
      scala_version = scala_version
    )
  } else if (method == "gateway") {
    scon <- gateway_connection(master = master, config = config)
  } else if (method == "databricks") {
    scon <- databricks_connection(
      config = config,
      extensions
    )
  } else if (method == "test") {
    scon <- test_connection(
      master = master,
      config = config,
      app_name,
      version,
      hadoop_version,
      extensions
    )
  } else if (method == "synapse") {
    scon <- synapse_connection(
      spark_home = spark_home,
      spark_version = version,
      scala_version = scala_version,
      config = config,
      extensions = extensions
    )
  } else {
    # other methods

    stop("Unsupported connection method '", method, "'")
  }

  scon$state$hive_support_enabled <- spark_config_value(
    config,
    name = "sparklyr.connect.enablehivesupport",
    default = TRUE
  )
  scon <- initialize_connection(scon)

  # initialize extensions
  if (length(scon$extensions) > 0) {
    for (initializer in scon$extensions$initializers) {
      if (is.function(initializer)) initializer(scon)
    }
  }

  # register mapping tables for spark.ml
  register_mapping_tables()

  # custom initializers for connection methods
  scon <- initialize_method(structure(scon, class = method), scon)

  # cache spark web
  scon$state$spark_web <- tryCatch(spark_web(scon), error = function(e) NULL)

  # notify connection viewer of connection
  libs <- c("sparklyr", extensions)
  libs <- vapply(libs,
    function(lib) paste0("library(", lib, ")"),
    character("1"),
    USE.NAMES = FALSE
  )
  libs <- paste(libs, collapse = "\n")
  if ("package:dplyr" %in% search()) {
    libs <- paste(libs, "library(dplyr)", sep = "\n")
  }
  parentCall <- match.call()
  connectCall <- paste(libs,
    paste("sc <-", deparse(parentCall, width.cutoff = 500), collapse = " "),
    sep = "\n"
  )

  # let viewer know that we've opened a connection; guess that the result will
  # be assigned into the global environment
  on_connection_opened(scon, globalenv(), connectCall)

  # Register a finalizer to sleep on R exit to support older versions of the RStudio IDE
  reg.finalizer(asNamespace("sparklyr"), function(x) {
    if (connection_is_open(scon)) {
      Sys.sleep(1)
    }
  }, onexit = TRUE)

  if (method == "databricks-connect") {
    spark_context(scon) %>% invoke("setLocalProperty", "spark.databricks.service.client.type", "sparklyr")
  }

  # add to our internal list
  spark_connections_add(scon)

  # return scon
  scon
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
  tryCatch(
    {
      subclass <- remove_class(sc, "spark_connection")
      spark_disconnect(subclass, ...)
    },
    error = function(err) {
    }
  )

  spark_connections_remove(sc)

  on_connection_closed(sc)

  stream_unregister_all(sc)

  # support custom operations after spark-submit useful to do custom cleanup in k8s
  invisible(spark_config_value(sc$config, c("sparklyr.connect.ondisconnect")))
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
  spark_master_is_local(sc$master) && !identical(sc$method, "databricks-connect")
}

spark_master_is_local <- function(master) {
  grepl("^local(\\[[0-9\\*]*\\])?$", master, perl = TRUE)
}

spark_connection_in_driver <- function(sc) {
  # is the current connection running inside the driver node?
  spark_connection_is_local(sc) || spark_connection_is_yarn_client(sc)
}

spark_connection_is_yarn <- function(sc) {
  grepl("^yarn(-client|-cluster)?$", sc$master, ignore.case = TRUE, perl = TRUE)
}

spark_connection_is_yarn_client <- function(sc) {
  grepl("^yarn-client$", sc$master, ignore.case = TRUE, perl = TRUE) ||
    (
      grepl("^yarn$", sc$master, ignore.case = TRUE, perl = TRUE) &&
        !identical(sc$config$`sparklyr.shell.deploy-mode`, "cluster")
    )
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

#' Terminate all Spark connections
#'
#' Call `spark_disconnect()` on each open Spark connection
#'
#' @param ... Additional params to be passed to each `spark_disconnect()` call
#'    (e.g., `terminate = TRUE`)
#'
#' @name spark-connections
#' @export
spark_disconnect_all <- function(...) {
  scons <- spark_connection_find_scon(function(e) {
    connection_is_open(e)
  })

  length(lapply(scons, function(e) {
    spark_disconnect(e, ...)
  }))
}

spark_inspect <- function(jobj) {
  print(jobj)
  if (!connection_is_open(spark_connection(jobj))) {
    return(jobj)
  }

  class <- invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- invoke(class, "getDeclaredFields")
  lapply(fields, function(field) {
    print(field)
  })

  cat("Methods:\n")
  methods <- invoke(class, "getDeclaredMethods")
  lapply(methods, function(method) {
    print(method)
  })

  jobj
}

initialize_method <- function(method, scon) {
  UseMethod("initialize_method")
}

initialize_method.default <- function(method, scon) {
  scon
}

#' Return the port number of a {sparklyr} backend.
#'
#' Retrieve the port number of the {sparklyr} backend associated with a Spark
#' connection.
#'
#' @param sc A \code{spark_connection}.
#'
#' @return The port number of the {sparklyr} backend associated with \code{sc}.
#'
#' @export
sparklyr_get_backend_port <- function(sc) {
  invoke_static(sc, "sparklyr.Shell", "getBackend") %>%
    invoke("getPort")
}
