context("ml aft survival regression")

sc <- testthat_spark_connection()

test_that("ml_aft_survival_regression param setting", {
  args <- list(
    x = sc, censor_col = "ccol",
    quantile_probabilities = c(0.01, 0.25, 0.5, 0.75, 0.95),
    fit_intercept = FALSE, max_iter = 50, tol = 1e-04,
    aggregation_depth = 3, quantiles_col = "qcol",
    label_col = "col", features_col = "fcol", prediction_col = "pcol"
  )
  ovr <- do.call(ml_aft_survival_regression, args)
  expect_equal(ml_params(ovr, names(args)[-1]), args[-1])
})

test_that("ml_aft_survival_regression() default params are correct", {

  predictor <- ml_pipeline(sc) %>%
    ml_aft_survival_regression() %>%
    ml_stage(1)

  args <- get_default_args(ml_aft_survival_regression,
                           c("x", "uid", "...", "quantiles_col")) %>%
    lapply(eval)

  expect_equal(
    within(ml_params(predictor, names(args)),
           quantile_probabilities <- as.list(quantile_probabilities)),
    args)
})
