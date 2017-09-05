context("ml validation")

sc <- testthat_spark_connection()

test_that("ml_cross_validator() parses grid correctly", {
  pipeline <- ml_tokenizer(sc, input_col = "text", output_col = "words") %>%
    ml_hashing_tf(input_col = "words", output_col = "features",
                  uid = "hashing_tf_1") %>%
    ml_logistic_regression(max_iter = 10, lambda = 0.001, uid = "logistic_1")
  param_grid <- list(
    hash = list(
      num_features = list(2^5, 2^10)
    ),
    logistic = list(
      max_iter = list(5, 10),
      lambda = list(0.01, 0.001)
    )
  )
  param_grid_full_stage_names <- list(
    hashing_tf_1 = list(
      num_features = list(2^5, 2^10)
    ),
    logistic_1 = list(
      max_iter = list(5, 10),
      lambda = list(0.01, 0.001)
    )
  )
  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    evaluator = invoke_new(sc, "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"),
    estimator_param_maps = param_grid
  )
  expect_equal(
    ml_param_grid(param_grid_full_stage_names),
    cv$estimator_param_maps
  )
})
