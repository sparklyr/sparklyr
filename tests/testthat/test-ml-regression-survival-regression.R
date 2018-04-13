context("ml regression - aft survival regression")

sc <- testthat_spark_connection()

test_that("ml_aft_survival_regression param setting", {
  args <- list(
    x = sc, censor_col = "ccol",
    quantile_probabilities = c(0.01, 0.25, 0.5, 0.75, 0.95),
    fit_intercept = FALSE, max_iter = 50, tol = 1e-04,
    quantiles_col = "qcol",
    label_col = "col", features_col = "fcol", prediction_col = "pcol"
  )
  if (spark_version(sc) >= "2.1.0")
    args <- c(args, aggregation_depth = 3)

  ovr <- do.call(ml_aft_survival_regression, args)
  expect_equal(ml_params(ovr, names(args)[-1]), args[-1])
})

test_that("ml_aft_survival_regression() default params are correct", {

  predictor <- ml_pipeline(sc) %>%
    ml_aft_survival_regression() %>%
    ml_stage(1)

  args <- get_default_args(ml_aft_survival_regression,
                           c("x", "uid", "...", "quantiles_col"))

  if (spark_version(sc) < "2.1.0")
    args <- rlang::modify(args, aggregation_depth = NULL, family = NULL)

  args <- lapply(Filter(length, args), eval)

  expect_equal(
    within(ml_params(predictor, names(args)),
           quantile_probabilities <- as.list(quantile_probabilities)),
    args)
})

test_that("ml_aft_survival_regression() works properly", {

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
