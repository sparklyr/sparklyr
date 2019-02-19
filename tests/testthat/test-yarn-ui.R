context("yarn ui")

sc <- testthat_spark_connection()

test_that("'spark_connection_yarn_ui()' can build a default URL", {
  expect_true(
    nchar(spark_connection_yarn_ui(sc)) > 0
  )
})
