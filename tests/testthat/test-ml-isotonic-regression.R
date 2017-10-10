context("ml isotonic regression")

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
