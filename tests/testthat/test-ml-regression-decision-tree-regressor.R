skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_decision_tree_regressor() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_decision_tree_regressor)
})

test_that("ml_decision_tree_regressor() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    max_depth = 6,
    max_bins = 16,
    min_instances_per_node = 2,
    min_info_gain = 1e-3,
    seed = 42,
    variance_col = "vcol",
    cache_node_ids = TRUE,
    checkpoint_interval = 15,
    max_memory_in_mb = 512,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol"
  )
  test_param_setting(sc, ml_decision_tree_regressor, test_args)
})
