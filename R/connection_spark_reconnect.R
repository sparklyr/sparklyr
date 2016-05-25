
spark_reconnect_if_needed <- function(scon) {
  sconInst <- spark_connection_get_inst(scon)
  if (!spark_connection_is_open(scon) && scon$reconnect == TRUE && !identical(sconInst, NULL)) {
    installInfo <- spark_install_info(scon$version)

    sconInst <- start_shell(sconInst, installInfo, scon$packages)
    spark_connection_set_inst(scon, sconInst)

    sconInst <- spark_connection_attach_context(scon, sconInst)
    spark_connection_set_inst(scon, sconInst)

    on_connection_opened(scon, sconInst$connectCall)

    lapply(sconInst$onReconnect, function(onReconnect) {
      onReconnect(scon)
    })
  }
}

spark_connection_global_inst <- function(instances = NULL) {
  if (!exists(".rspark.connections", envir = globalenv())) {
    assign(".rspark.connections", list(), envir = globalenv())
  }

  if (!identical(instances, NULL)) {
    assign(".rspark.connections", instances, envir = globalenv())
  }

  get(".rspark.connections" , envir = globalenv())
}

spark_connection_get_inst <- function(scon) {
  instances <- spark_connection_global_inst()

  if (!identical(scon$sconRef, NULL)) instances[[scon$sconRef]] else NULL
}

spark_connection_add_inst <- function(sconInst) {
  instances <- spark_connection_global_inst()

  sconRef <- as.character(length(instances) + 1)
  instances[[sconRef]] <- sconInst

  spark_connection_global_inst(instances)
  sconRef
}

spark_connection_set_inst <- function(scon, sconInst) {
  instances <- spark_connection_global_inst()

  instances[[scon$sconRef]] <- sconInst

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
