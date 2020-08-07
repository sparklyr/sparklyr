initialize_method.qubole <- function(method, scon) {
  scon$config$sparklyr.web.spark <- invoke(
    scon$state$spark_context,
    "getSparkUIURL"
  )
  yarn_url <- Sys.getenv("QUBOLE_YARN_WEB")
  scon$config$sparklyr.web.yarn <- if (nchar(yarn_url) > 0) {
    yarn_url
  } else {
    system("/usr/lib/rstudio-utils/bin/get-yarn-url.sh",
      intern = TRUE
    )
  }
  scon
}
