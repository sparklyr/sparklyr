context("ml classification - multilayer perceptron")

sc <- testthat_spark_connection()

test_that("ml_multilayer_perceptron_classifier() parses params correctly", {

  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    label_col = "lcol", layers = c(20, 40, 20),
    max_iter = 90, seed = 56,
    tol = 1e-05, block_size = 256
  ) %>%
    param_add_version("2.0.0", solver = "gd",
                      step_size = 0.01)

  mlpc <- do.call(ml_multilayer_perceptron_classifier, args)
  expect_equal(ml_params(mlpc, names(args)[-1]), args[-1])
})

test_that("ml_multilayer_perceptron() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_multilayer_perceptron(layers = c(2, 2)) %>%
    ml_stage(1)

  args <- get_default_args(ml_multilayer_perceptron,
                           c("x", "uid", "...", "initial_weights", "seed",
                             "layers", "response", "features")) %>%
    param_filter_version("2.0.0", c("solver", "step_size"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_multilayer_perceptron returns correct number of weights", {
  iris_tbl <- testthat_tbl("iris")
  mlp <- ml_multilayer_perceptron_classifier(
    iris_tbl, formula = "Species ~ .", seed = 42,
    layers = c(4, 10, 3)
  )
  expect_equal(length(mlp$model$weights), 4 * 10 + 10 + 10 * 3 + 3)
})
