context("ml decision tree")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("ml_decision_tree_regressor() param parsing works" , {
  dtr <- ml_decision_tree_regressor(
    sc, features_col = "fcol", label_col = "lcol", prediction_col = "pcol",
    variance_col = "vcol", checkpoint_interval = 11, impurity = "variance",
    max_bins = 64, max_depth = 1, min_info_gain = 0.001,
    min_instances_per_node = 2, seed = 42, cache_node_ids = TRUE,
    max_memory_in_mb = 128
  )

  expect_equal(ml_params(dtr, list(
    "features_col", "label_col", "prediction_col", "variance_col", "checkpoint_interval",
    "impurity", "max_bins", "max_depth", "min_info_gain", "min_instances_per_node",
    "seed", "cache_node_ids", "max_memory_in_mb"
  )),
  list(
    features_col = "fcol", label_col = "lcol", prediction_col = "pcol",
    variance_col = "vcol", checkpoint_interval = 11L, impurity = "variance",
    max_bins = 64L, max_depth = 1L, min_info_gain = 0.001,
    min_instances_per_node = 2L, seed = 42L, cache_node_ids = TRUE,
    max_memory_in_mb = 128L
  ))
})

test_that("ml_decision_tree_classifier() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_decision_tree_classifier() %>%
    ml_stage(1)

  args <- get_default_args(ml_decision_tree_classifier,
                           c("x", "uid", "...", "thresholds", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_decision_tree_regressor() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_decision_tree_regressor() %>%
    ml_stage(1)

  args <- get_default_args(ml_decision_tree_regressor,
                           c("x", "uid", "...", "variance_col", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_decision_tree_classifier() param parsing works" , {
  dtc <- ml_decision_tree_classifier(
    sc, features_col = "fcol", label_col = "lcol", prediction_col = "pcol",
    probability_col = "prcol", raw_prediction_col = "rpcol",
    checkpoint_interval = 11, impurity = "gini",
    max_bins = 64, max_depth = 1, min_info_gain = 0.001, thresholds = c(0.1, 0.3, 0.5),
    min_instances_per_node = 2, seed = 42, cache_node_ids = TRUE,
    max_memory_in_mb = 128
  )

  expect_equal(ml_params(dtc, list(
    "features_col", "label_col", "prediction_col", "probability_col", "raw_prediction_col",
    "checkpoint_interval", "impurity", "max_bins", "max_depth", "min_info_gain",
    "thresholds", "min_instances_per_node",
    "seed", "cache_node_ids", "max_memory_in_mb"
  )),
  list(
    features_col = "fcol", label_col = "lcol", prediction_col = "pcol",
    probability_col = "prcol", raw_prediction_col = "rpcol",
    checkpoint_interval = 11L, impurity = "gini",
    max_bins = 64L, max_depth = 1L, min_info_gain = 0.001, thresholds = c(0.1, 0.3, 0.5),
    min_instances_per_node = 2L, seed = 42L, cache_node_ids = TRUE,
    max_memory_in_mb = 128L
  ))
})

test_that("decision tree runs successfully when all args specified", {
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
                       type = "classification",
                       impurity = "entropy", max.bins = 16L,
                       max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                       thresholds = c(1/2, 1/3, 1/4), seed = 42L),
    NA
  )
})

test_that("thresholds parameter behaves as expected", {
  test_requires("dplyr")
  most_predicted_label <- function(x) x %>%
    count(prediction) %>%
    arrange(desc(n)) %>%
    pull(prediction) %>%
    first()

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(0, 1, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 0)

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 1)

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 1, 0)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 2)
})

test_that("error for thresholds with wrong length", {
  if (spark_version(sc) < "2.1.0") skip("threshold length checking implemented in 2.1.0")
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for bad impurity specification", {
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "'impurity' must be 'gini' or 'entropy' for classification"
  )

  expect_error(
    iris_tbl %>%
      ml_decision_tree(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "'impurity' must be 'variance' for regression"
  )
})
