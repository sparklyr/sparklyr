context("ml classification")

sc <- testthat_spark_connection()

test_that("ml_logistic_regression() returns params", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, alpha = 0)
  params <- list(intercept = TRUE, alpha = 0)
  params_from_object <- lr$stages$logreg$params
  expect_equal(list(), setdiff(params, params_from_object))
})
