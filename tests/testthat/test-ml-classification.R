context("ml classification")

sc <- testthat_spark_connection()

test_that("ml_logistic_regression() returns params", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, elastic_net_param = 0)
  expected_params <- list(intercept = TRUE, elastic_net_param = 0)
  params <- lr$param_map
  expect_equal(setdiff(expected_params, params), list())
})

test_that("ml_logistic_regression() does input checking", {
  expect_error(ml_logistic_regression(sc, elastic_net_param = "foo"),
               "length-one numeric vector")
  expect_equal(ml_logistic_regression(sc, max_iter = 25)$param_map$max_iter,
               25L)
})
