Sys.setenv("R_TESTS" = "")

library(testthat)
library(sparklyr)

test_check("testthat")

spark_disconnect_all()
