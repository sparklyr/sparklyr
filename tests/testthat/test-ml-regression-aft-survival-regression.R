skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("ml_aft_survival_regression() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_aft_survival_regression)
})

test_that("ml_aft_survival_regression() param setting", {
  test_requires_version("3.0.0")
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

sc <- testthat_spark_connection()

training <- data.frame(
  label = c(1.218, 2.949, 3.627, 0.273, 4.199),
  censor = c(1.0, 0.0, 0.0, 1.0, 0.0),
  V1 = c(1.560, 0.346, 1.380, 0.520, 0.795),
  V2 = c(-0.605, 2.158, 0.231, 1.151, -0.226)
)

training_tbl <- sdf_copy_to(sc, training, overwrite = TRUE)

test_that("ml_aft_survival_regression() works properly", {
  training_va <- ft_vector_assembler(
    training_tbl,
    c("V1", "V2"), "features"
  )

  aft <- ml_aft_survival_regression(
    training_va,
    quantile_probabilities = list(0.3, 0.6),
    quantiles_col = "quantiles"
  )

  expect_equal(aft$coefficients, c(-0.49631114666506776, 0.19844437699934067), tolerance = 1e-4, scale = 1)
  expect_equal(aft$intercept, 2.6380946151040043, tolerance = 1e-4, scale = 1)
  expect_equal(aft$scale, 1.5472345574364683, tolerance = 1e-4, scale = 1)

  predicted_tbl <- ml_predict(aft, training_va)

  expect_warning_on_arrow(
    p_q <- predicted_tbl %>%
      dplyr::pull(quantiles) %>%
      dplyr::first()
  )

  expect_equal(p_q,
    c(1.1603238947151593, 4.995456010274735),
    tolerance = 1e-4, scale = 1
  )
  expect_equal(predicted_tbl %>%
    dplyr::pull(prediction) %>%
    dplyr::first(),
  5.718979487634966,
  tolerance = 1e-4, scale = 1
  )

  aft_model <- ml_aft_survival_regression(training_va, label ~ V1 + V2, features_col = "feat")

  expect_equal(coef(aft_model),
    structure(c(2.63808989630564, -0.496304411053117, 0.198452172529228), .Names = c("(Intercept)", "V1", "V2")),
    tolerance = 1e-05, scale = 1
  )
})

test_that("ML Pipeline works", {
  aft_pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(c("V1", "V2"), "features") %>%
    ml_aft_survival_regression(
      quantile_probabilities = list(0.3, 0.6),
      quantiles_col = "quantiles"
    )

  expect_is(aft_pipeline, "ml_pipeline")

  aft_fitted <- ml_fit(aft_pipeline, training_tbl)

  expect_is(aft_fitted, "ml_pipeline_model")
})

test_that("Deprecated function fails", {
  sc <- testthat_spark_connection()
  expect_warning(
    ml_survival_regression(sc),
    "'ml_survival_regression' is deprecated."
    )
})

test_that("Tuning works with AFT", {
  test_requires("survival")

  sc <- testthat_spark_connection()

  ovarian_tbl <- sdf_copy_to(
    sc,
    survival::ovarian,
    name = "ovarian_tbl",
    overwrite = TRUE
    )

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(futime ~ ecog_ps + rx + age + resid_ds) %>%
    ml_aft_survival_regression(censor_col = "fustat")

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      aft_survival_regression = list(
        max_iter = c(10, 20),
        aggregation_depth = c(2, 4)
      )
    ),
    evaluator = ml_regression_evaluator(sc),
    num_folds = 2,
    seed = 1111
  )

  cv_model <- ml_fit(cv, ovarian_tbl)
  expect_is(cv_model, "ml_cross_validator_model")

  cv_metrics <- ml_validation_metrics(cv_model)
  expect_equal(dim(cv_metrics), c(4, 3))
})


