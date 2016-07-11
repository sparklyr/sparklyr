
sparkConnectionsEnv <- new.env(parent = emptyenv())

spark_connection_instances <- function() {
  if (is.null(sparkConnectionsEnv$instances))
    sparkConnectionsEnv$instances <- list()

  sparkConnectionsEnv$instances
}

spark_connections_add <- function(sc) {
  instances <- spark_connection_instances()
  instances[[length(instances) + 1]] <- sc
  sparkConnectionsEnv$instances <- instances
}

spark_connections_remove <- function(sc) {
  instances <- spark_connection_instances()
  for (i in 1:length(instances)) {
    if (sc$master == instances[[i]]$master) {
      instances[[i]] <- NULL
      sparkConnectionsEnv$instances <- instances
      break
    }
  }
}

spark_connection_find_scon <- function(test) {
  Filter(function(e) { test(e) }, spark_connection_instances())
}
