initialize_method.qubole <- function(method, scon) {
  scon$config$sparklyr.web.spark <- invoke(scon$state$spark_context,
                                             "getSparkUIURL")
  scon$config$sparklyr.web.yarn <- system("/usr/lib/rstudio-utils/bin/get-yarn-url.sh",
                                            intern = TRUE)
  scon
}