Sys.setenv("R_TESTS" = "")

library(testthat)
library(sparklyr)

test_check("testthat")

on.exit({
  spark_disconnect_all()
  Sys.sleep(3)
})
