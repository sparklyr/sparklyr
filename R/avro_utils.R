#' @include spark_connection.R
#' @include spark_invoke.R

spark_avro_package_name <- function(spark_version, scala_version = NULL) {
  if (is.null(spark_version)) {
    stop(
      "Avro requires Spark version to be specified explicitly with ",
      "spark_connect(..., version = <version>, ...)"
    )
  }

  if (spark_version < "2.4.0") stop("Avro requires Spark 2.4.0 or newer")

  scala_version <- scala_version %||% (
    if (spark_version >= "3.0.0") "2.12" else "2.11"
  )
  paste0(
    "org.apache.spark:spark-avro_",
    scala_version,
    ":",
    spark_version
  )
}

validate_spark_avro_pkg_version <- function(sc) {
  # get the full Spark version including possible suffixes such as "-preview"
  full_spark_version <- invoke(spark_context(sc), "version")
  spark_avro_pkg <- spark_avro_package_name(full_spark_version)
  if (!spark_avro_pkg %in% sc$config$`sparklyr.shell.packages`) {
    stop(
      "Avro support must be enabled with ",
      "`spark_connect(..., version = <version>, packages = c(\"avro\", <other package(s)>), ...)` ",
      " or by explicitly including '", spark_avro_pkg, "' for Spark version ",
      full_spark_version, " in list of packages"
    )
  }
}
