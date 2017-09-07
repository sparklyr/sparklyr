Sys.setenv("R_TESTS" = "")

library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  spark_install("2.1.0")

  test_check("sparklyr", filter = "livy")

  on.exit({
    spark_disconnect_all()
    livy_service_stop()
    Sys.sleep(3)
  })
}
