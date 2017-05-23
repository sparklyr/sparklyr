spark_worker_apply <- function(sc) {
  spark_context <- invoke_static(sc, "sparklyr.Backend", "getSparkContext")
  log("retrieved context")

  spark_split <- invoke_static(sc, "sparklyr.WorkerRDD", "getSplit")
  log("retrieved split")
}
