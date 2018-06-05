context("ml supervised - gradient boosted trees")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("ml_gbt_classifier() parses params correctly (<2.2)", {
  if (spark_version(sc) >= "2.2.0") skip("")
  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    # probability_col = "prcol", raw_prediction_col = "rpcol",
    checkpoint_interval = 9, loss_type = "logistic",
    max_bins = 30, max_depth = 6,
    max_iter = 19, min_info_gain = 0.01, min_instances_per_node = 2,
    step_size = 0.01, subsampling_rate = 0.9, seed = 42,
    # thresholds = c(0.1, 0.3, 0.6),
    cache_node_ids = TRUE,
    max_memory_in_mb = 128
  )
  gbtc <- do.call(ml_gbt_classifier, args)
  expect_equal(ml_params(gbtc, names(args)[-1]), args[-1])
})

test_that("ml_gbt_classifier() parses params correctly (>=2.3)", {
  test_requires_version("2.3.0")
  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    probability_col = "prcol", raw_prediction_col = "rpcol",
    checkpoint_interval = 9, loss_type = "logistic",
    max_bins = 30, max_depth = 6,
    max_iter = 19, min_info_gain = 0.01, min_instances_per_node = 2,
    step_size = 0.01, subsampling_rate = 0.9,
    feature_subset_strategy = "onethird", seed = 42,
    thresholds = c(0.1, 0.3, 0.6),
    cache_node_ids = TRUE,
    max_memory_in_mb = 128
  )
  gbtc <- do.call(ml_gbt_classifier, args)
  expect_equal(ml_params(gbtc, names(args)[-1]), args[-1])
})

test_that("ml_gbt_classifier() default params are correct (<2.2)", {
  if (spark_version(sc) >= "2.2.0") skip("")
  predictor <- ml_pipeline(sc) %>%
    ml_gbt_classifier() %>%
    ml_stage(1)

  args <- get_default_args(ml_gbt_classifier,
                           c("x", "uid", "...", "seed", "thresholds", "probability_col",
                             "raw_prediction_col", "feature_subset_strategy"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_gbt_classifier() default params are correct (>= 2.2.0)", {
  test_requires_version("2.3.0")
  predictor <- ml_pipeline(sc) %>%
    ml_gbt_classifier() %>%
    ml_stage(1)

  args <- get_default_args(ml_gbt_classifier,
                           c("x", "uid", "...", "seed", "thresholds"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_gbt_regressor() parses params correctly", {
  test_requires_version("2.3.0")
  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    checkpoint_interval = 9, loss_type = "absolute",
    max_bins = 30, max_depth = 6,
    max_iter = 19, min_info_gain = 0.01, min_instances_per_node = 2,
    step_size = 0.01, subsampling_rate = 0.9,
    feature_subset_strategy = "onethird", seed = 42,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128
  )
  gbtr <- do.call(ml_gbt_regressor, args)
  expect_equal(ml_params(gbtr, names(args)[-1]), args[-1])
})

test_that("ml_gbt_regressor() default params are correct", {
  test_requires_version("2.3.0")
  predictor <- ml_pipeline(sc) %>%
    ml_gbt_regressor() %>%
    ml_stage(1)

  args <- get_default_args(ml_gbt_regressor,
                           c("x", "uid", "...", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_gbt_regressor() parases params correct", {
  args <- list(
    x = sc, features_col = "fcol", prediction_col = "pcol",
    checkpoint_interval = 8, loss_type = "absolute", max_bins = 30,
    max_depth = 4, max_iter = 25, min_info_gain = 0.1,
    min_instances_per_node = 2L, step_size = 0.015,
    subsampling_rate = 0.3, seed = 43, cache_node_ids = TRUE,
    max_memory_in_mb = 300
  )
  gbtr <- do.call(ml_gbt_regressor, args)
  expect_equal(ml_params(gbtr, names(args)[-1]), args[-1])
})

test_that("gbt runs successfully when all args specified", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
                              type = "classification",
                              max.bins = 16L,
                              max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                              seed = 42L)
  expect_equal(class(model)[1], "ml_model_gbt_classification")
})

test_that("thresholds parameter behaves as expected", {
  if (spark_version(sc) < "2.2.0") skip("thresholds not supported for GBT for Spark <2.2.0")
  test_requires("dplyr")
  most_predicted_label <- function(x) x %>%
    count(prediction) %>%
    arrange(desc(n)) %>%
    pull(prediction) %>%
    first()

  gbt_predictions <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(0, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(gbt_predictions), 0)

  gbt_predictions <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(gbt_predictions), 1)
})

test_that("informative error when using Spark version that doesn't support thresholds", {
  test_requires("dplyr")
  if (spark_version(sc) >= "2.2.0") skip("not applicable, threshold is supported")
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                                thresholds = c(0, 1)),
    "thresholds is only supported for GBT in Spark 2.2.0\\+"
  )
})

test_that("one-tree ensemble agrees with ml_decision_tree()", {
  test_requires("dplyr")
  gbt <- iris_tbl %>%
    ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression",
                     sample.rate = 1,
                     num.trees = 1L)
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression")

  expect_equal(gbt %>%
                 sdf_predict(iris_tbl) %>%
                 collect(),
               dt %>%
                 sdf_predict(iris_tbl) %>%
                 collect())
})

test_that("checkpointing works for gbt", {
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                                type = "regression",
                                cache.node.ids = TRUE,
                                checkpoint.interval = 5L),
    NA
  )
})

test_that('ml_gradient_boosted_trees() supports response-features syntax', {
  iris_tbl <- testthat_tbl("iris")
  ml_gradient_boosted_trees(iris_tbl,
                            response = 'Sepal_Length',
                            features = c('Sepal_Width', 'Petal_Length'))
})
