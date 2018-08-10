context("ml regression - aft survival regression")

test_that("ml_aft_survival_regression() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_aft_survival_regression)
})

test_that("ml_aft_survival_regression() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    censor_col = "ccol",
    quantile_probabilities = c(0.02, 0.5, 0.98),
    fit_intercept = FALSE,
    max_iter = 42,
    tol = 1e-04,
    aggregation_depth = 3,
    quantiles_col = "qcol",
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol"
  )
  test_param_setting(sc, ml_aft_survival_regression, test_args)
})

test_that("ml_aft_survival_regression() works properly", {
  sc <- testthat_spark_connection()
  training <- data.frame(
    label = c(1.218, 2.949, 3.627, 0.273, 4.199),
    censor = c(1.0, 0.0, 0.0, 1.0, 0.0),
    V1 = c(1.560, 0.346, 1.380, 0.520, 0.795),
    V2 = c(-0.605, 2.158, 0.231, 1.151, -0.226)
  )
  training_tbl <- sdf_copy_to(sc, training, overwrite = TRUE) %>%
    ft_vector_assembler(c("V1", "V2"), "features")

  aft <- ml_aft_survival_regression(training_tbl, quantile_probabilities = list(0.3, 0.6),
                             quantiles_col = "quantiles")

  expect_equal(aft$coefficients, c(-0.49631114666506776, 0.19844437699934067), tolerance = 1e-4)
  expect_equal(aft$intercept, 2.6380946151040043, tolerance = 1e-4)
  expect_equal(aft$scale, 1.5472345574364683, tolerance = 1e-4)

  predicted_tbl <- ml_predict(aft, training_tbl)
  expect_equal(predicted_tbl %>%
                 dplyr::pull(quantiles) %>%
                 dplyr::first(),
               c(1.1603238947151593, 4.995456010274735),
               tolerance = 1e-4)
  expect_equal(predicted_tbl %>%
                 dplyr::pull(prediction) %>%
                 dplyr::first(),
               5.718979487634966, tolerance = 1e-4)

  aft_model <- ml_aft_survival_regression(training_tbl, label ~ V1 + V2, features_col = "feat")
  expect_equal(coef(aft_model),
               structure(c(2.63808989630564, -0.496304411053117, 0.198452172529228
               ), .Names = c("(Intercept)", "V1", "V2")),
               tolerance = 1e-05)
})
