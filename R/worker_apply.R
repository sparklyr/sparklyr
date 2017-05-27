spark_worker_apply <- function(sc) {
  spark_split <- worker_invoke_static(sc, "SparkWorker.WorkerRDD", "getSplit")
  log("retrieved split")

  spark_split <- worker_invoke_static(sc, "SparkWorker.WorkerRDD", "finish")
  log("finished apply")
}
