

#' @import sparkapi
NULL

# register the spark_connection S3 class for use in setClass slots
methods::setOldClass("spark_connection")

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

  # prepare windows environment
  prepare_windows_environment()

  # master can be missing if it's specified in the config file
  if (missing(master)) {
    master <- config$spark.master
    if (is.null(master))
      stop("You must either pass a value for master or include a spark.master ",
           "entry in your config.yml")
  }
  
  # determine whether we need cores in master
  cores <- config[["sparklyr.cores.local"]]
  if (master == "local" && !identical(cores, NULL))
    master <- paste("local[", cores, "]", sep = "")

  filter <- function(e) {
    connection_is_open(e) &&
    identical(e$master, master) &&
    identical(e$app_name, app_name)
  }

  sconFound <- spark_connection_find_scon(filter)
  if (length(sconFound) == 1) {
    message("Reusing existing connection to: ",
            master,
            ". If you need a new connection to this cluster use a different app_name.")
    return(sconFound[[1]])
  }

  # verify that java is available
  if (!is_java_available()) {
    stop("Java is required to connect to Spark. Please download and install Java from ",
         java_install_url())
  }

  # value for SPARK_HOME from environment
  sparkHome <- spark_home()
  
  # warn if a SPARK_HOME was specified along with a local version
  if (!is.null(sparkHome) && (!is.null(version) || !is.null(hadoop_version))) {
    message("Using SPARK_HOME specified in environment rather than ",
            "local version specified in spark_connect")
  }
  
  # if no SPARK_HOME and local then resolve from local installs
  if (is.null(sparkHome) && spark_master_is_local(master)) {
    installInfo <- spark_install_find(version, hadoop_version, latest = FALSE, connecting = TRUE)
    sparkHome <- installInfo$sparkVersionDir  
  }
  
  # error if there is no SPARK_HOME
  if (is.null(sparkHome))
    stop("Failed to connect to Spark (SPARK_HOME is not set).")
  
  scon <- NULL
  tryCatch({
    # determine environment
    environment <- character()
    if (.Platform$OS.type != "windows") {
      if (spark_master_is_local(master))
        environment <- paste0("SPARK_LOCAL_IP=127.0.0.1")
    }

    # determine shell_args (use fake connection b/c we don't yet
    # have a real connection)
    config_sc <- list(config = config, master = master)
    shell_args <- connection_config(config_sc, "sparklyr.shell.")

    # create connection
    Sys.setenv(SPARK_HOME = sparkHome)
    scon <- sparkapi::start_shell(
      master = master,
      app_name = app_name,
      config = config,
      jars = spark_default_jars(),
      packages = config[["sparklyr.defaultPackages"]],
      extensions = extensions,
      environment = environment,
      shell_args = shell_args
    )
    
    # mark the connection as a DBIConnection class to allow DBI to use defaults
    attr(scon, "class") <- c(attr(scon, "class"), "DBIConnection")
    
    # update spark_context and hive_context connections with DBIConnection
    scon$spark_context$connection <- scon
    scon$hive_context$connection <- scon
   
    # notify connection viewer of connection
    libs <- "library(sparklyr)"
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
  }, error = function(err) {
    tryCatch({
      spark_log(scon)
    }, error = function(e) {
    })
    stop(err)
  })

  # add to our internal list
  spark_connections_add(scon)
  
  # return scon
  scon
}

#' @docType NULL
#' @name spark_log
#' @rdname spark_log
#' 
#' @export
sparkapi::spark_log

#' @docType NULL
#' @name spark_web
#' @rdname spark_web
#' 
#' @export
sparkapi::spark_web


#' Check if a Spark connection is open
#' 
#' @param sc \code{spark_connection}
#' 
#' @rdname spark_connect
#' 
#' @export
spark_connection_is_open <- function(sc) {
  sparkapi::connection_is_open(sc)
}


#' Disconnect from Spark
#'
#' @param sc Spark connection provided by \code{\link{spark_connect}}
#'
#' @family Spark connections
#'
#' @export
spark_disconnect <- function(sc) {
  tryCatch({
    sparkapi::stop_shell(sc)
  }, error = function(err) {
  })

  spark_connections_remove(sc)
  
  on_connection_closed(sc)
}


# Get the path to a temp file containing the current spark log (used by IDE)
spark_log_file <- function(sc) {
  scon <- sc
  if (!connection_is_open(scon)) {
    stop("The Spark conneciton is not open anymmore, log is not available")
  }
  
  lines <- spark_log(sc, n = NULL)
  tempLog <- tempfile(pattern = "spark", fileext = ".log")
  writeLines(tempLog)
  
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

