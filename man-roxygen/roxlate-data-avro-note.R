#' @note Notice this functionality requires the Spark connection \code{sc} to be
#'   instantiated with either an explicitly specified Spark version (i.e.,
#'   \code{spark_connect(..., version = <version>, packages = c("avro", <other package(s)>), ...)})
#'   or a specific version of Spark avro package to use (e.g.,
#'   \code{spark_connect(..., packages = c("org.apache.spark:spark-avro_2.12:3.0.0", <other package(s)>), ...)}).
