
sparkConnectionsEnv <- new.env(parent = emptyenv())

spark_connection_instances <- function() {
  if (is.null(sparkConnectionsEnv$instances)) {
    sparkConnectionsEnv$instances <- list()
  }

  sparkConnectionsEnv$instances
}

spark_connections_add <- function(sc) {
  instances <- spark_connection_instances()
  instances[[length(instances) + 1]] <- sc
  sparkConnectionsEnv$instances <- instances
}

spark_connections_remove <- function(sc) {
  instances <- spark_connection_instances()
  for (i in seq_along(instances)) {
    if (sc$master == instances[[i]]$master) {
      instances[[i]] <- NULL
      sparkConnectionsEnv$instances <- instances
      break
    }
  }
}

spark_connection_find_scon <- function(test) {
  Filter(function(e) {
    test(e)
  }, spark_connection_instances())
}

#' Find Spark Connection
#'
#' Finds an active spark connection in the environment given the
#' connection parameters.
#'
#' @param master The Spark master parameter.
#' @param app_name The Spark application name.
#' @param method The method used to connect to Spark.
#'
#' @export
spark_connection_find <- function(master = NULL,
                                  app_name = NULL,
                                  method = NULL) {
  filter <- function(e) {
    connection_is_open(e) &&
      (identical(method, NULL) || identical(e$method, method)) &&
      (identical(master, NULL) || identical(e$master, master)) &&
      (identical(app_name, NULL) || identical(e$app_name, app_name))
  }

  spark_connection_find_scon(filter)
}
