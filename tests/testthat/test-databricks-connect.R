context("databricks-connect")

test_that("spark connection works", {
  sc <- testthat_spark_connection()
  expect_equal(sc$method, "databricks-connect")
})
