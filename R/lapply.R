#' @import sparklyr
#' @export
spark_lapply <- function(x, f) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  schema <- invoke(sdf, "schema")
  rdd <- invoke_static(sc, "SparkWorker.WorkerHelper", "computeRdd", sdf)
  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}
