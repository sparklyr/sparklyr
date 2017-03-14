#' @import sparklyr
#' @export
sdf_lapply <- function(sc, sdf) {
  parent <- invoke(sdf, "rdd")
  schema <- invoke(sdf, "schema")
  rdd <- invoke_new(sc, "SparkWorker.WorkerRDD", parent)
  invoke(hive_context(sc), "createDataFrame", rdd, schema)
}
