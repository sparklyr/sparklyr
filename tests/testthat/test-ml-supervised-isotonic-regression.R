context("ml regression - isotonic regression")

sc <- testthat_spark_connection()

test_that("ml_isotonic_regression param setting", {
  args <- list(
    x = sc, feature_index = 1, isotonic = FALSE, weight_col = "wcol",
    label_col = "col", features_col = "fcol", prediction_col = "pcol"
  )
  predictor <- do.call(ml_isotonic_regression, args)
  expect_equal(ml_params(predictor, names(args)[-1]), args[-1])
})

test_that("ml_isotonic_regression() default params are correct", {

  predictor <- ml_pipeline(sc) %>%
    ml_isotonic_regression() %>%
    ml_stage(1)

  args <- get_default_args(ml_isotonic_regression,
                           c("x", "uid", "...", "weight_col"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_isotonic_regression() works properly", {
  test_requires("dplyr")
  df <- data.frame(
    x = 1:9,
    y = c(1, 2, 3, 1, 6, 17, 16, 17, 18)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  ir <- ml_isotonic_regression(df_tbl, y ~ x)
  expect_equal(
    sdf_predict(ir, df_tbl) %>% pull(prediction),
    c(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18)
  )

  expect_equal(
    ir$model$boundaries,
    c(1, 2, 4, 5, 6, 7, 8, 9)
  )

  expect_equal(
    ir$model$predictions,
    c(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0)
  )
})
