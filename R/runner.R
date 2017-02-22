#' @import sparklyr
#' @export
spark_run <- function(sc) {
  invoke_static(sc, "SparkWorker.Runner", "run")
}
