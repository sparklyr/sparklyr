context("ml classification decision tree")

test_that("ml_decision_tree_classifier() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_decision_tree_classifier)
})

test_that("ml_decision_tree_classifier() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    max_depth = 6,
    max_bins = 16,
    min_instances_per_node = 2,
    min_info_gain = 1e-3,
    impurity = "entropy",
    seed = 42,
    thresholds = c(0.3, 0.6),
    cache_node_ids = TRUE,
    checkpoint_interval = 15,
    max_memory_in_mb = 512,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol",
    probability_col = "prcol",
    raw_prediction_col = "rpcol"
  )
  test_param_setting(sc, ml_decision_tree_classifier, test_args)
})
