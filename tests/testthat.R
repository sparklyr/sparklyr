Sys.setenv("R_TESTS" = "")

library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  test_check("testthat")

  on.exit({
    spark_disconnect_all()
    Sys.sleep(3)
  })
}
