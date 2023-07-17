skip_connection("ml-regression-random-forest-regressor")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_random_forest_regressor() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_random_forest_regressor)
})

test_that("ml_random_forest_regressor() param setting", {
  test_requires_version("3.0.0")
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

test_that("Tuning works RF", {
  sc <- testthat_spark_connection()

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_random_forest_regressor()

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      random_forest_regressor = list(
        max_depth = c(5, 10)
      )
    ),
    evaluator = ml_regression_evaluator(sc),
    num_folds = 2,
    seed = 1111
  )

  cv_model <- ml_fit(cv, testthat_tbl("iris"))
  expect_is(cv_model, "ml_cross_validator_model")

  cv_metrics <- ml_validation_metrics(cv_model)
  expect_equal(dim(cv_metrics), c(2, 2))
})
