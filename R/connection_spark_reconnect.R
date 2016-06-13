
spark_reconnect_if_needed <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)
  if (!spark_connection_is_open(scon) && scon$reconnect == TRUE && !identical(sconInst, NULL)) {
    installInfo <- spark_install_find(scon$sparkVersion, scon$hadoopVersion)

    sconInst <- start_shell(scon, sconInst)
    spark_connection_set_inst(scon, sconInst)

    sconInst <- spark_connection_attach_context(scon, sconInst)
    spark_connection_set_inst(scon, sconInst)

    on_connection_opened(scon, sconInst$connectCall)

    lapply(sconInst$onReconnect, function(onReconnect) {
      onReconnect(scon)
    })
  }
}

sparkConnectionsEnv <- new.env(parent = emptyenv())

spark_connection_global_inst <- function(instances = NULL) {
  if (!identical(instances, NULL)) {
    sparkConnectionsEnv$.rspark.connections <- instances
  }

  sparkConnectionsEnv$.rspark.connections
}

spark_connection_get_inst <- function(scon) {
  instances <- spark_connection_global_inst()

  if (!identical(scon$sconRef, NULL)) instances[[scon$sconRef]]$sconInst else NULL
}

spark_connection_add_inst <- function(master, appName, scon, sconInst) {
  instances <- spark_connection_global_inst()

  scon$sconRef <- as.character(length(instances) + 1)

  instances[[scon$sconRef]] <- list(
    master = master,
    appName = appName,
    scon = scon,
    sconInst = sconInst
  )

  spark_connection_global_inst(instances)
  scon
}

spark_connection_set_inst <- function(scon, sconInst) {
  instances <- spark_connection_global_inst()

  if (is.null(sconInst)) {
    instances[[scon$sconRef]] <- NULL
  }
  else {
    instances[[scon$sconRef]]$sconInst <- sconInst
  }

  spark_connection_global_inst(instances)

  scon$sconRef
}

spark_connection_remove_inst <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)
  spark_connection_set_inst(scon, NULL)

  sconInst
}

#' Provides an extension mechanism to allow package builders to support spark_connect(reconnect = TRUE)
#' @param scon Spark connection provided by spark_connect
#' @param onReconnect A function with signature onReconnect(scon, sconInst) that returns an updated sconInst
spark_connection_on_reconnect <- function(scon, onReconnect) {
  sconInst <- spark_connection_get_inst(scon)
  sconInst$onReconnect[[length(sconInst$onReconnect) + 1]] <- onReconnect
  spark_connection_set_inst(scon, sconInst)
}

spark_connection_find_scon <- function(test) {
  instances <- spark_connection_global_inst()
  instances <- Filter(function(e) { test(e$scon) }, instances)

  lapply(instances, function(e) { e$scon })
}

