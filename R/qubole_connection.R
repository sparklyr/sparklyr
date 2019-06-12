initialize_method.qubole <- function(method, scon) {
  scon$config$sparklyr.web.spark <- invoke(scon$state$spark_context,
                                             "getSparkUIURL")
  scon$config$sparklyr.web.yarn <- system("/usr/lib/zeppelin/bin/get_yarn_url.sh",
                                            intern = TRUE)
  scon
}