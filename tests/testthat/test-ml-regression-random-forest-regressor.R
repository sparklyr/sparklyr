context("ml regression random forest regressor")

test_that("ml_random_forest_regressor() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_random_forest_regressor)
})

test_that("ml_random_forest_regressor() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    num_trees = 30,
    subsampling_rate = 0.3,
    max_depth = 10,
    min_instances_per_node = 2,
    feature_subset_strategy = "sqrt",
    impurity = "variance",
    min_info_gain = 0.01,
    max_bins = 12,
    seed = 55,
    checkpoint_interval = 14,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "featureswaef",
    label_col = "asdfawf",
    prediction_col = "efwf"
  )
  test_param_setting(sc, ml_random_forest_regressor, test_args)
})
