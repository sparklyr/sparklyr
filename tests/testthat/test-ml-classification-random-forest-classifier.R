context("ml classification random forest classifier")

test_that("ml_random_forest_classifier() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_random_forest_classifier)
})

test_that("ml_random_forest_classifier() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    num_trees = 10,
    subsampling_rate = 0.4,
    max_depth = 9,
    min_instances_per_node = 2,
    feature_subset_strategy = "sqrt",
    impurity = "entropy",
    min_info_gain = 0.01,
    max_bins = 14,
    seed = 3145,
    thresholds = c(0.4, 0.6),
    checkpoint_interval = 15,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "featuresawefawef",
    label_col = "labelfqf",
    prediction_col = "predictionqweqwd",
    probability_col = "probabilityasdf",
    raw_prediction_col = "rawPredictionwefawf"
  )
  test_param_setting(sc, ml_random_forest_classifier, test_args)
})
