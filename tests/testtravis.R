parent_dir <- dir("../", full.names = TRUE)
sparklyr_package <- parent_dir[grepl("sparklyr_", parent_dir)]
install.packages(sparklyr_package, repos = NULL, type = "source")

Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  test_check("sparklyr")
  on.exit({ spark_disconnect_all() ; livy_service_stop() })
}
