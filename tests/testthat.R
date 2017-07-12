Sys.setenv("R_TESTS" = "")

library(testthat)
library(sparklyr)

test_check("testthat")
testthat_spark_terminate()
