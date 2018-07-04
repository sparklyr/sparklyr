Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # test_check("sparklyr")
  testthat::test_file("testthat/test-spark-apply.R")

  on.exit({ spark_disconnect_all() ; livy_service_stop() })
}
