context("ml classification")

sc <- testthat_spark_connection()

test_that("ml_logistic_regression() returns params", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, alpha = 0)
  expected_params <- list(intercept = TRUE, alpha = 0)
  params <- lr$param_map
  expect_equal(setdiff(expected_params, params), list())
})
