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

test_that("we can cross validate a logistic regression with xval", {
  if (spark_version(sc) < "2.1.0") skip("multinomial logistic regression not supported")
  iris_tbl <- testthat_tbl("iris")

  pipeline <- ml_pipeline(sc, uid = "pipeline_1") %>%
    ft_r_formula(Species ~ Petal_Width + Petal_Length, dataset = iris_tbl,
                 uid = "r_formula_1") %>%
    ml_logistic_regression(uid = "logreg_1")

  bad_grid <- list(
    logistic = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )

  expect_error(
    ml_cross_validator(
      iris_tbl, estimator = pipeline, estimator_param_maps = bad_grid,
      evaluator = ml_multiclass_classification_evaluator(sc),
      seed = 1, uid = "cv_1"),
    "The name logistic matches no stages in pipeline"
  )

  grid <- list(
    logreg_1 = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )

  cvm <- ml_cross_validator(
    iris_tbl, estimator = pipeline, estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc, uid = "eval1"),
    seed = 1, uid = "cv_1")

  expect_identical(names(cvm$avg_metrics_df),
               c("f1", "elastic_net_param_S1", "reg_param_S1"))
  expect_identical(nrow(cvm$avg_metrics_df), 4L)
  summary_string <- capture.output(summary(cvm)) %>%
    paste0(collapse = "\n")

  expect_match(summary_string,
               "3-fold cross validation")
  expect_output_file(
    print(cvm),
    output_file("print/cross-validator-model.txt")
  )
})

test_that("we can train a regression with train-validation-split", {
  if (spark_version(sc) < "2.1.0") skip("multinomial logistic regression not supported")
  iris_tbl <- testthat_tbl("iris")

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Species ~ Petal_Width + Petal_Length, dataset = iris_tbl) %>%
    ml_logistic_regression()

  grid <- list(
    logistic = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )
  tvsm <- ml_train_validation_split(iris_tbl, estimator = pipeline, estimator_param_maps = grid,
                            evaluator = ml_multiclass_classification_evaluator(sc),
                            seed = 1)

  expect_identical(names(tvsm$validation_metrics_df),
                   c("f1", "elastic_net_param_S1", "reg_param_S1"))
  expect_identical(nrow(tvsm$validation_metrics_df), 4L)
  summary_string <- capture.output(summary(tvsm)) %>%
    paste0(collapse = "\n")

  expect_match(summary_string,
               "0\\.75/0\\.25 train-validation split")
})
