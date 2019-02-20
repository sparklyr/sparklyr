context("ml regression - isotonic regression")

test_that("ml_isotonic_regression() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_isotonic_regression)
})

test_that("ml_isotonic_regression() param setting", {
  test_requires_latest_spark()
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
