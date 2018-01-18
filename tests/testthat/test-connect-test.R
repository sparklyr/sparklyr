context("connections - test")
sc <- testthat_spark_connection()

test_that("test connection does not fail", {
  skip_on_cran()

  sc <- spark_connect(master = "test", method = "test")

  expect_true(!is.null(sc))
})
