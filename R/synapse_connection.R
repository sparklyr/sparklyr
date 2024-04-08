synapse_connection <- function(spark_home,
                               spark_version,
                               scala_version,
                               config,
                               extensions) {
  sc <- list()
  spark_version <- spark_version_from_home(spark_home, default = spark_version)
  jar_path <- spark_default_app_jar(spark_version, scala_version = scala_version)
  verbose <- spark_config_value(config, "sparklyr.verbose", FALSE)
  connector_class <- spark_config_value(config, "sparklyr.connector.className", "org.apache.spark.sparklyr.SparklyrConnector")
  tryCatch(
    {
      call_sparkr_static <- get("callJStatic", envir = asNamespace("SparkR"))
      call_sparkr_method <- get("callJMethod", envir = asNamespace("SparkR"))

      if (verbose) {
        message("[Synapse] Start sparklyr.Backend")
      }

      sparklyr_connector <- call_sparkr_static(
        connector_class,
        "getOrCreate"
      )

      if (verbose) {
        message(sprintf("[Synapse] Add sparklyr jar: %s", jar_path))
      }

      call_sparkr_method(
        sparklyr_connector,
        "addJar",
        jar_path
      )

      if (verbose) {
        message("[Synapse] Start sparklyr backend")
      }

      call_sparkr_method(
        sparklyr_connector,
        "start"
      )

      gateway_uri <- call_sparkr_method(
        sparklyr_connector,
        "getUri"
      )

      if (verbose) {
        message(sprintf("[Synapse] Connect to sparklyr gateway: %s", gateway_uri))
      }

      sc <- new_spark_gateway_connection(
        gateway_connection(
          gateway_uri,
          config = config
        ),
        class = "synapse_connection"
      )

      if (verbose) {
        message("[Synapse] Connect to existing spark session")
      }

      sc$stat$hive_context <- invoke(
        invoke_static(sc, "org.apache.spark.sql.SparkSession", "builder"),
        "getOrCreate"
      )

      if (verbose) {
        message("[Synapse] Sucessfully connect to spark")
      }
    },
    error = function(err) {
      stop("[Synapse] Failed to start sparklyr backend: ", err$message)
    }
  )

  sc
}

#' @export
spark_version.synapse_connection <- function(sc) {
  if (!is.null(sc$state$spark_version)) {
    return(sc$state$spark_version)
  }

  version <- eval(rlang::parse_expr("SparkR::sparkR.version()"))

  sc$state$spark_version <- numeric_version(version)

  # return to caller
  sc$state$spark_version
}
