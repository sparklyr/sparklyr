

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

  # prepare windows environment
  prepare_windows_environment()

  # master can be missing if it's specified in the config file
  if (missing(master)) {
    master <- config$spark.master
    if (is.null(master))
      stop("You must either pass a value for master or include a spark.master ",
           "entry in your config.yml")
  }

  filter <- function(e) {
    connection_is_open(e) &&
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

  scon <- NULL
  tryCatch({

    # determine whether we need cores in master
    cores <- config[["sparklyr.cores.local"]]
    if (master == "local" && !identical(cores, NULL))
      master <- paste("local[", cores, "]", sep = "")

    # determine environment
    environment <- character()
    if (.Platform$OS.type != "windows") {
      if (spark_master_is_local(master))
        environment <- paste0("SPARK_LOCAL_IP=127.0.0.1")
    }

    # determine shell_args
    shell_args <- read_config(config, master, "sparklyr.shell.")

    # create connection
    scon <- sparkapi::start_shell(
      master = master,
      spark_home = sparkHome,
      app_name = app_name,
      config = spark_config,
      jars = spark_default_jars(),
      packages = config[["sparklyr.defaultPackages"]],
      extensions = extensions,
      environment = environment,
      shell_args = shell_args
    )

    # add sparklyr_connection class to object (reflects inclusion of db fields)
    class(scon) <- c("sparklyr_connection", class(scon))
    
    # attach hive_context or sql_context as a fallback
    scon$hive_context <- spark_api_create_hive_context(scon)
    if (is.null(scon$hive_context)) {
      warning("Failed to create Hive context, falling back to SQL. Some operations, ",
              "like window-funcitons, will not work")
      scon$sql_context <- spark_api_create_sql_context(scon)
      if (is.null(scon$sql_context))
        stop("Failed to create SQL context")
    }
    
    # create dbi interface
    api <- spark_api_create(scon)
    scon$dbi <- new("DBISparkConnection", scon = scon, api = api)
    params <- read_config(scon$config, scon$master, "spark.sql.")
    lapply(names(params), function(paramName) {
      dbSetProperty(scon$dbi, paramName, as.character(params[[paramName]]))
    })
    
    # update spark_context connection with fields we've added
    scon$spark_context$connection$hive_context <- scon$hive_context
    scon$spark_context$connection$dbi <- scon$dbi
   
    # update hive_context / sql_context with fields we've added
    if (!is.null(scon$hive_context)) {
      scon$hive_context$connection$hive_context <- scon$hive_context
      scon$hive_context$connection$dbi <- scon$dbi
    } else {
      scon$sql_context$connection$sql_context <- scon$sql_context
      scon$sql_context$connection$dbi <- scon$dbi
    }
    
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


spark_connection_version <- function(sc, onlyVersion = FALSE) {
  rawVersion <- invoke(spark_context(sc), "version")

  # Get rid of -preview and other suffix variations if needed
  if (onlyVersion) gsub("([0-9]+\\.?)[^0-9\\.](.*)","\\1", rawVersion) else rawVersion
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
