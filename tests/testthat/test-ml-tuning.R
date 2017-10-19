context("ml tuning")

sc <- testthat_spark_connection()

test_that("ml_cross_validator() works correctly", {
  pipeline <- ml_pipeline(sc) %>%
    ft_tokenizer("text", "words", uid = "tokenizer_1") %>%
    ft_hashing_tf("words", "features", uid = "hashing_tf_1") %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001, uid = "logistic_1")
  param_grid <- list(
    hash = list(
      num_features = list(2^5, 2^10)
    ),
    logistic = list(
      max_iter = list(5, 10),
      reg_param = list(0.01, 0.001)
    )
  )
  param_grid_full_stage_names <- list(
    hashing_tf_1 = list(
      num_features = list(as.integer(2^5), as.integer(2^10))
    ),
    logistic_1 = list(
      max_iter = list(5L, 10L),
      reg_param = list(0.01, 0.001)
    )
  )
  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    evaluator = ml_binary_classification_evaluator(sc, label_col = "label",
                                                   raw_prediction_col = "rawPrediction"),
    estimator_param_maps = param_grid
  )

  expected_param_maps <- param_grid_full_stage_names %>%
    sparklyr:::ml_expand_params() %>%
    sparklyr:::ml_build_param_maps()

  list_sorter <- function(l) {
    l[sort(names(l))]
  }

  diff <- mapply(function(x, y) mapply(function(a, b) setdiff(a, b), x, y),
         lapply(expected_param_maps, list_sorter),
         lapply(cv$estimator_param_maps, list_sorter), SIMPLIFY = FALSE)

  expect_identical(
    c(diff),
    rep(list(list(hashing_tf_1 = list(),
         logistic_1 = list())), 8)
  )

  expect_identical(
    class(cv),
    c("ml_cross_validator", "ml_tuning", "ml_estimator", "ml_pipeline_stage")
  )
})
