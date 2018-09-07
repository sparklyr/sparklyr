spark_config_settings <- function(name = NULL) {
  settings <- list(
    sparklyr.app.jar = "",
    sparklyr.apply.packages = "",
    sparklyr.apply.schema.infer = "",
    sparklyr.apply.schema.infer = "",
    sparklyr.backend.interval = "",
    sparklyr.backend.interval = "",
    sparklyr.backend.interval = "",
    sparklyr.backend.timeout = "",
    sparklyr.closures.rlang = "",
    sparklyr.cores = "",
    sparklyr.cores.local = "",
    sparklyr.defaultPackages = "",
    sparklyr.events.aftersubmit = "",
    sparklyr.gateway.address = "",
    sparklyr.gateway.address = "",
    sparklyr.gateway.config.retries = "",
    sparklyr.gateway.connect.timeout = "",
    sparklyr.gateway.connect.timeout = "",
    sparklyr.gateway.interval = "",
    sparklyr.gateway.port = "",
    sparklyr.gateway.port = "",
    sparklyr.gateway.port = "",
    sparklyr.gateway.routing = "",
    sparklyr.gateway.start.timeout = "",
    sparklyr.gateway.start.timeout = "",
    sparklyr.gateway.start.wait = "",
    sparklyr.invoke.trace = "",
    sparklyr.invoke.trace.callstack = "",
    sparklyr.jars.default = "",
    sparklyr.log.console = "",
    sparklyr.progress = "",
    sparklyr.progress = "",
    sparklyr.progress.interval = "",
    sparklyr.progress.stages = "",
    "sparklyr.spark-submit" = "",
    sparklyr.stream.collect.timeout = "",
    sparklyr.stream.validate.timeout = "",
    sparklyr.worker.gateway.address = "",
    sparklyr.worker.gateway.address = "",
    sparklyr.worker.gateway.port = "",
    sparklyr.worker.gateway.port = "",
    sparklyr.yarn.cluster.accepted.timeout = "",
    sparklyr.yarn.cluster.hostaddress.timeout = "",
    sparklyr.yarn.cluster.lookup.byname = "",
    sparklyr.yarn.cluster.lookup.prefix = "",
    sparklyr.yarn.cluster.lookup.username = "",
    sparklyr.yarn.cluster.start.timeout = ""
  )

  if (!is.null(name) && !name %in% names(settings) && getOption("sparklyr.test.enforce.config")) {
    stop("Config value '", name, "' not described in spark_config_description()")
  }

  data.frame(
    name = names(settings),
    description = unlist(unname(settings))
  )
}
