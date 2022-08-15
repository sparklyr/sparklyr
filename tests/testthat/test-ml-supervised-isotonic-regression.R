skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_isotonic_regression() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_isotonic_regression)
})

test_that("ml_isotonic_regression() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    feature_index = 1,
    isotonic = FALSE,
    weight_col = "wcol",
    features_col = "fcol",
    label_col = "lalefa",
    prediction_col = "wefwef"
  )
  test_param_setting(sc, ml_isotonic_regression, test_args)
})

test_that("ml_isotonic_regression() works properly", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    x = 1:9,
    y = c(1, 2, 3, 1, 6, 17, 16, 17, 18)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  ir <- ml_isotonic_regression(df_tbl, y ~ x)
  expect_equal(
    ml_predict(ir, df_tbl) %>% pull(prediction),
    c(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18)
  )

  expect_equal(
    ir$model$boundaries(),
    c(1, 2, 4, 5, 6, 7, 8, 9)
  )

  expect_equal(
    ir$model$predictions(),
    c(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0)
  )
})

test_that("Tuning works Isotonic", {
  sc <- testthat_spark_connection()

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_isotonic_regression()

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      isotonic_regression = list(
        isotonic = c(TRUE, FALSE)
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
