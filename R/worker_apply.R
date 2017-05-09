spark_worker_apply <- function(sc) {
  spark_context <- invoke_static(sc, "sparklyr.Backend", "getSparkContext")
  log("sparklyr worker retrieved context")
}
