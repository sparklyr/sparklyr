context("ml classification gbt classifier")

test_that("ml_gbt_classifier() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_gbt_classifier)
})

test_that("ml_gbt_classifier() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    max_iter = 10,
    max_depth = 6,
    step_size = 0.14,
    subsampling_rate = 0.5,
    feature_subset_strategy = "sqrt",
    min_instances_per_node = 2,
    max_bins = 15,
    min_info_gain = 0.01,
    loss_type = "logistic",
    seed = 123123,
    thresholds = c(0.3, 0.7),
    checkpoint_interval = 11,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol",
    probability_col = "prcol",
    raw_prediction_col = "rpcol"
  )
  test_param_setting(sc, ml_gbt_classifier, test_args)
})
