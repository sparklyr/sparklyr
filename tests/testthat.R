Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # enforce all configuration settings are described
  options(sparklyr.test.enforce.config = TRUE)

  test_filter <- NULL

  livy_version <- Sys.getenv("LIVY_VERSION")
  r_arrow <- Sys.getenv("R_ARROW")
  if (nchar(livy_version) > 0 || isTRUE(as.logical(r_arrow))) {
    if (isTRUE(as.logical(r_arrow))) {
      get("library")("arrow")
    }

    livy_tests <- c(
      "^dplyr$",
      "^dbi$",
      "^copy-to$",
      "^spark-apply$",
      "^ml-clustering-kmeans$"
    )

    test_filter <- paste(livy_tests, sep = "|")
  }

  test_check("sparklyr", filter = test_filter)

  on.exit({ spark_disconnect_all() ; livy_service_stop() })
}
