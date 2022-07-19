skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_gbt_regressor() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_gbt_regressor)
})

test_that("ml_gbt_regressor() param setting", {
  test_requires_version("3.0.0")
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
    loss_type = "absolute",
    seed = 123123,
    checkpoint_interval = 11,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "fcol",
    label_col = "lcol"
  )
  test_param_setting(sc, ml_gbt_regressor, test_args)
})

test_that("ML Pipeline works for GBTs", {

  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  m_pipeline <- ml_pipeline(sc) %>%
    ml_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_gbt_regressor()

  expect_is(m_pipeline, "ml_pipeline")

  m_fitted <- ml_fit(m_pipeline, iris_tbl)

  expect_is(m_fitted, "ml_pipeline_model")
})


test_that("Tuning works GBT", {
  sc <- testthat_spark_connection()

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_gbt_regressor()

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      gbt_regressor = list(
        max_depth = c(5, 10),
        max_bins = c(32, 40)
      )
    ),
    evaluator = ml_regression_evaluator(sc),
    num_folds = 2,
    seed = 1111
  )

  cv_model <- ml_fit(cv, testthat_tbl("iris"))
  expect_is(cv_model, "ml_cross_validator_model")

  cv_metrics <- ml_validation_metrics(cv_model)
  expect_equal(dim(cv_metrics), c(4, 3))
})
