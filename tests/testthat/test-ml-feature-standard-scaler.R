skip_connection("ml-feature-standard-scaler")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_standard_scaler() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_standard_scaler)
})

test_that("ft_standard_scaler() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    with_mean = TRUE,
    with_std = TRUE
  )
  test_param_setting(sc, ft_standard_scaler, test_args)
})

test_that("ft_standard_scaler() works properly", {
  sc <- testthat_spark_connection()
  sample_data_path <- get_test_data_path("sample_libsvm_data.txt")
  sample_data <- spark_read_libsvm(
    sc,
    "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  scaler <- ft_standard_scaler(
    sc,
    input_col = "features",
    output_col = "scaledFeatures",
    with_std = TRUE,
    with_mean = FALSE,
    uid = "standard_scalaer_999"
  )

  scaler_model <- ml_fit(scaler, sample_data)

  expect_warning_on_arrow(
    s_m <- scaler_model %>%
      ml_transform(sample_data) %>%
      head(1) %>%
      dplyr::pull(scaledFeatures) %>%
      unlist() %>%
      sum()
  )

  expect_equal(
    s_m,
    295.3425,
    tolerance = 0.001,
    scale = 1
  )

  expect_output_file(
    print(scaler_model),
    output_file("print/standard-scaler-model.txt")
  )
})

test_that("ft_standard_scaler() survives ml_cross_validator (tuning/reflection)", {
  sc <- testthat_spark_connection()
  test_requires_version("3.0.0")
  iris_tbl <- testthat_tbl("iris")

  # A pure feature stage can't be cross-validated alone (CV needs an evaluator,
  # which needs predictions), so the scaler sits upstream of a model in the tuned
  # pipeline. This asserts the migrated estimator survives the
  # tuning -> fit -> reflect-back path with its specific class and mean/std
  # accessors intact -- i.e. that keeping new_ml_standard_scaler_model matters.
  pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(
      c("Sepal_Length", "Sepal_Width", "Petal_Length"),
      "features_raw"
    ) %>%
    ft_standard_scaler(
      "features_raw",
      "features",
      with_mean = TRUE,
      uid = "ss_canary"
    ) %>%
    ft_string_indexer("Species", "label") %>%
    ml_logistic_regression(uid = "lr_canary")

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(lr_canary = list(reg_param = list(0, 0.1))),
    evaluator = ml_multiclass_classification_evaluator(sc),
    num_folds = 2,
    seed = 1
  )

  cv_model <- ml_fit(cv, iris_tbl)

  best <- cv_model$best_model
  expect_true(inherits(best, "ml_pipeline_model"))
  ss_stage <- ml_stage(best, "ss_canary")
  # reflection must rebuild the *specific* fitted-model class + accessors,
  # not the generic ml_transformer fallback
  expect_true(inherits(ss_stage, "ml_standard_scaler_model"))
  expect_false(is.null(ss_stage$mean))
  expect_false(is.null(ss_stage$std))
})

test_clear_cache()
