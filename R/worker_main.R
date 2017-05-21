spark_worker_main <- function(sessionId) {
  log("sparklyr worker starting")

  sc <- spark_worker_connect(sessionId)

  log("sparklyr worker connected")

  spark_worker_apply(sc)

  log("sparklyr worker finished")
}
