context("yarn ui")

sc <- testthat_spark_connection()

test_that("'spark_connection_yarn_ui()' can build a default URL", {
  Sys.setenv(
    YARN_CONF_DIR = dirname(dir(getwd(), recursive = TRUE, pattern = "yarn-site.xml", full.names = TRUE))
  )

  expect_true(
    nchar(spark_connection_yarn_ui(sc)) > 0
  )
})
