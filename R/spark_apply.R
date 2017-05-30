#' @import sparklyr
#' @export
spark_apply <- function(x, f) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  closure <- serialize(f, NULL)

  schema <- invoke(sdf, "schema")
  rdd <- invoke_static(sc, "SparkWorker.WorkerHelper", "computeRdd", sdf, closure)
  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}
