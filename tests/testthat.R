Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # enforce all configuration settings are described
  options(sparklyr.test.enforce.config = TRUE)

  test_filter <- NULL

  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0) {
    livy_tests <- c(
      "^dplyr$",
      "^dbi$",
      "^copy-to$",
      "^spark-apply$",
      "^ml-clustering-kmeans$"
    )

    test_filter <- paste(livy_tests, collapse = "|")
  }

  r_arrow <- isTRUE(as.logical(Sys.getenv("R_ARROW")))
  if (r_arrow) {
    get("library")("arrow")
  }

  on.exit({ spark_disconnect_all() ; livy_service_stop() })

  test_check("sparklyr", filter = test_filter)
}
