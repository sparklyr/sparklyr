Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  options(repos=structure(c(CRAN="https://cran.rstudio.com/")))

  test_check("sparklyr")
  on.exit({ spark_disconnect_all() ; livy_service_stop() })
}
