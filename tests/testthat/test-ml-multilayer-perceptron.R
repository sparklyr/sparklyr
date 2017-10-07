context("ml multilayer perceptron")

sc <- testthat_spark_connection()

test_that("ml_multilayer_perceptron_classifier() parses params correctly", {

  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    label_col = "lcol", layers = c(20, 40, 20),
    max_iter = 90, seed = 56, step_size = 0.01,
    tol = 1e-05, block_size = 256, solver = "gd"
  )
  mlpc <- do.call(ml_multilayer_perceptron_classifier, args)
  expect_equal(ml_params(mlpc, names(args)[-1]), args[-1])
})

test_that("ml_multilayer_perceptron returns correct number of weights", {
  iris_tbl <- testthat_tbl("iris")
  mlp <- ml_multilayer_perceptron_classifier(
    iris_tbl, formula = "Species ~ .", seed = 42,
    layers = c(4, 10, 3)
  )
  expect_equal(length(mlp$model$weights), 4 * 10 + 10 + 10 * 3 + 3)
})
