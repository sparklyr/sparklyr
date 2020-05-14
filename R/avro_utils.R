spark_avro_package_name <- function(spark_version) {
  if (is.null(spark_version))
    stop("Avro requires Spark version to be specified explicitly with ",
         "spark_connect(..., version = <version>, ...)")

  if (spark_version < "2.4.0") stop("Avro requires Spark 2.4.0 or newer")

  scala_version <- if (spark_version >= "3.0.0") "2.12" else "2.11"
  paste0(
    "org.apache.spark:spark-avro_",
    scala_version,
    ":",
    spark_version
  )
}
