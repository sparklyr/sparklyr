#' @import sparklyr
#' @export
spark_lapply <- function(sc) {
  rdd <- invoke_new(sc, "SparkWorker.WorkerRDD")
  spark_invoke_static(sc, "SparkWorker.Utils", "rddToDF", rdd)
}
