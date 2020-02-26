context("ml classification - multilayer perceptron")

test_that("ml_multilayer_perceptron_classifier() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_multilayer_perceptron_classifier)
})

test_that("ml_multilayer_perceptron_classifier() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    layers = c(6, 32, 64, 32),
    max_iter = 50,
    step_size = 0.01,
    tol = 1e-5,
    block_size = 256,
    solver = "gd",
    seed = 34534,
    initial_weights = 1:10,
    features_col = "fosadf",
    label_col = "wefwfe"
  )
  test_param_setting(sc, ml_multilayer_perceptron_classifier, test_args)
})

test_that("ml_multilayer_perceptron returns correct number of weights", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  mlp <- ml_multilayer_perceptron_classifier(
    iris_tbl, formula = "Species ~ .", seed = 42,
    layers = c(4, 10, 3)
  )
  expect_equal(length(mlp$model$weights), 4 * 10 + 10 + 10 * 3 + 3)
})
