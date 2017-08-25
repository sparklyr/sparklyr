context("ml classification")

sc <- testthat_spark_connection()

test_that("ml_logistic_regression() returns params", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, alpha = 0, name = "lr")
  params <- list(intercept = TRUE, alpha = 0)
  expect_true(dplyr::setequal(lr$stages$lr$params, params))
})
