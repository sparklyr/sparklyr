skip_connection("ml-feature-robust-scaler")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_robust_scaler() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("3.0.0", "ft_robust_scaler requires Spark 3.0.0+")
  df <- data.frame(
    id = 1:5,
    V1 = c(0.0, 1.0, 2.0, 3.0, 4.0),
    V2 = c(0.0, -1.0, -2.0, -3.0, -4.0)
  )

  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    r_s <- df_tbl %>%
      ft_vector_assembler(paste0("V", 1:2), "features") %>%
      ft_robust_scaler("features", "scaled") %>%
      pull(scaled)
  )

  expect_equal(
    r_s,
    list(
      c(-1, 1),
      c(-0.5, 0.5),
      c(0, 0),
      c(0.5, -0.5),
      c(1, -1)
    )
  )
})

test_that("ft_robust_scaler() survives ml_cross_validator (tuning/reflection)", {
  sc <- testthat_spark_connection()
  test_requires_version("3.0.0", "ft_robust_scaler requires Spark 3.0.0+")
  iris_tbl <- testthat_tbl("iris")

  # The scaler sits upstream of a tuned model so the fitted RobustScalerModel is
  # rebuilt by reflection (ml_call_constructor) on the tuning -> fit -> reflect-back
  # path. Asserts the fitted stage carries its specific *_model class rather than
  # the estimator class / generic transformer fallback -- this is the gate for the
  # new_ml_robust_scaler_model class fix and the RobustScalerModel class mapping.
  pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(
      c("Sepal_Length", "Sepal_Width", "Petal_Length"),
      "features_raw"
    ) %>%
    ft_robust_scaler("features_raw", "features", uid = "rs_canary") %>%
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
  rs_stage <- ml_stage(best, "rs_canary")
  expect_true(inherits(rs_stage, "ml_robust_scaler_model"))
  expect_true(is_ml_transformer(rs_stage))
})

test_clear_cache()
