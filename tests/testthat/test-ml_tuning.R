# ---------------------------------------------------------------------------
# Connection-free tests for the tuning helpers, validators, and print/summary
# code. The live CV/TVS fits are skip_slow (excluded from coverage), so cover the
# pure logic, the error branches, and the model print/summary here (mocking the
# JVM-facing bits).
# ---------------------------------------------------------------------------

test_that("ml_validate_params() errors on ambiguous / missing stage names", {
  expect_error(
    ml_validate_params(
      list(foo = list(list(p = 1))),
      list(foo1 = NULL, foo2 = NULL),
      list()
    ),
    "matches more than one stage"
  )
  expect_error(
    ml_validate_params(
      list(bar = list(list(p = 1))),
      list(foo1 = NULL),
      list()
    ),
    "matches no stages"
  )
})

test_that("validate_args_tuning() rejects bad estimator/param_maps/evaluator", {
  base <- list(collect_sub_models = FALSE, parallelism = 1, seed = NULL)
  expect_error(
    validate_args_tuning(c(base, list(estimator = structure(1, class = "x")))),
    "must be an .ml_estimator"
  )
  expect_error(
    validate_args_tuning(c(base, list(estimator_param_maps = 5))),
    "must be a list"
  )
  expect_error(
    validate_args_tuning(c(base, list(evaluator = structure(1, class = "x")))),
    "must be an .ml_evaluator"
  )
})

test_that("ml_validation_metrics() dispatches by model class", {
  expect_equal(
    ml_validation_metrics(
      structure(list(avg_metrics_df = "A"), class = "ml_cross_validator_model")
    ),
    "A"
  )
  expect_equal(
    ml_validation_metrics(
      structure(
        list(validation_metrics_df = "B"),
        class = "ml_train_validation_split_model"
      )
    ),
    "B"
  )
  expect_error(
    ml_validation_metrics(structure(list(), class = "other")),
    "must be called on"
  )
})

test_that("ml_sub_models() errors when sub-models were not collected", {
  expect_error(
    ml_sub_models(structure(list(), class = "ml_cross_validator_model")),
    "collect_sub_models"
  )
})

skip_connection("ml_tuning")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("ml_cross_validator() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_cross_validator)
})

test_that("ml_cross_validator() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    num_folds = 10,
    collect_sub_models = TRUE,
    parallelism = 2,
    seed = 6543
  )
  test_param_setting(sc, ml_cross_validator, test_args)
})

test_that("ml_cross_validator() works correctly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.3.0")
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
    evaluator = ml_binary_classification_evaluator(
      sc,
      label_col = "label",
      raw_prediction_col = "rawPrediction"
    ),
    estimator_param_maps = param_grid,
    parallelism = 2
  )

  expect_identical(ml_param(cv, "parallelism"), 2L)

  expected_param_maps <- param_grid_full_stage_names %>%
    purrr::map(cross_compat) %>%
    cross_compat()

  list_sorter <- function(l) {
    l[sort(names(l))]
  }

  diff <- mapply(
    function(x, y) mapply(function(a, b) setdiff(a, b), x, y),
    lapply(expected_param_maps, list_sorter),
    lapply(cv$estimator_param_maps, list_sorter),
    SIMPLIFY = FALSE
  )

  expect_identical(
    c(diff),
    rep(
      list(list(
        hashing_tf_1 = list(),
        logistic_1 = list()
      )),
      8
    )
  )

  expect_identical(
    class(cv),
    c("ml_cross_validator", "ml_tuning", "ml_estimator", "ml_pipeline_stage")
  )
})

test_that("we can cross validate a logistic regression with xval", {
  skip_slow("takes too long to measure coverage")
  sc <- testthat_spark_connection()
  test_requires_version("2.3.0")
  iris_tbl <- testthat_tbl("iris")

  pipeline <- ml_pipeline(sc, uid = "pipeline_1") %>%
    ft_r_formula(Species ~ Petal_Width + Petal_Length, uid = "r_formula_1") %>%
    ml_logistic_regression(uid = "logreg_1")

  bad_grid <- list(
    logistic = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )

  expect_error(
    ml_cross_validator(
      iris_tbl,
      estimator = pipeline,
      estimator_param_maps = bad_grid,
      evaluator = ml_multiclass_classification_evaluator(sc),
      seed = 1,
      uid = "cv_1"
    ),
    "The name logistic matches no stages in the pipeline"
  )

  grid <- list(
    logreg_1 = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )

  cvm <- ml_cross_validator(
    iris_tbl,
    estimator = pipeline,
    estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc, uid = "eval1"),
    collect_sub_models = TRUE,
    seed = 1,
    uid = "cv_1"
  )

  expect_identical(
    sort(names(cvm$avg_metrics_df)),
    sort(c("f1", "elastic_net_param_1", "reg_param_1"))
  )
  expect_identical(nrow(cvm$avg_metrics_df), 4L)
  summary_string <- capture.output(summary(cvm)) %>%
    paste0(collapse = "\n")

  expect_match(
    summary_string,
    "3-fold cross validation"
  )

  sub_models <- ml_sub_models(cvm)
  expect_identical(length(unlist(sub_models, recursive = FALSE)), 12L)
})

test_that("ml_validation_metrics() works properly", {
  skip_slow("takes too long to measure coverage")
  sc <- testthat_spark_connection()
  test_requires_version("2.3.0")
  iris_tbl <- testthat_tbl("iris")

  labels <- c("setosa", "versicolor", "virginica")
  pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(
      c("Sepal_Width", "Sepal_Length", "Petal_Width", "Petal_Length"),
      "features"
    ) %>%
    ft_string_indexer_model("Species", "label", labels = labels) %>%
    ml_logistic_regression()

  grid <- list(
    logistic = list(
      elastic_net_param = c(0.25),
      reg_param = c(1e-3)
    )
  )
  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc),
    num_folds = 2,
    parallelism = 4
  )
  cv_model <- ml_fit(cv, iris_tbl)
  cv_metrics <- ml_validation_metrics(cv_model)

  tvs <- ml_train_validation_split(
    sc,
    estimator = pipeline,
    estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc),
    parallelism = 4
  )
  tvs_model <- ml_fit(tvs, iris_tbl)
  tvs_metrics <- ml_validation_metrics(tvs_model)

  expect_setequal(
    names(cv_metrics),
    c("f1", "elastic_net_param_1", "reg_param_1")
  )

  expect_identical(nrow(cv_metrics), 1L)

  expect_setequal(
    names(tvs_metrics),
    c("f1", "elastic_net_param_1", "reg_param_1")
  )

  expect_identical(nrow(tvs_metrics), 1L)
})

test_that("cross validator print methods", {
  sc <- testthat_spark_connection()
  lr <- ml_logistic_regression(sc, uid = "logistic")
  param_maps <- list(
    logistic = list(
      reg_param = c(0.1, 0.01),
      elastic_net_param = c(0.1, 0.2)
    )
  )
  evaluator <- ml_binary_classification_evaluator(sc, uid = "bineval")

  cv1 <- ml_cross_validator(sc, uid = "cv")

  cv2 <- ml_cross_validator(
    sc,
    estimator = lr,
    estimator_param_maps = param_maps,
    evaluator = evaluator,
    uid = "cv"
  )

  cv3 <- ml_cross_validator(
    sc,
    estimator = lr,
    estimator_param_maps = param_maps,
    uid = "cv"
  )

  expect_known_output(
    cv1,
    output_file("print/cv1.txt"),
    print = TRUE
  )

  expect_known_output(
    cv2,
    output_file("print/cv2.txt"),
    print = TRUE
  )

  expect_known_output(
    cv3,
    output_file("print/cv3.txt"),
    print = TRUE
  )
})

test_that("ml_train_validation_split() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_train_validation_split)
})

test_that("ml_train_validation_split() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    train_ratio = 0.5,
    collect_sub_models = TRUE,
    parallelism = 2,
    seed = 34535
  )
  test_param_setting(sc, ml_train_validation_split, test_args)
})

test_that("we can train a regression with train-validation-split", {
  sc <- testthat_spark_connection()
  test_requires_version("2.3.0")
  iris_tbl <- testthat_tbl("iris")

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Species ~ Petal_Width + Petal_Length) %>%
    ml_logistic_regression()

  grid <- list(
    logistic = list(
      reg_param = c(0, 0.01),
      elastic_net_param = c(0, 0.01)
    )
  )
  tvsm <- ml_train_validation_split(
    iris_tbl,
    estimator = pipeline,
    estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc),
    collect_sub_models = TRUE,
    seed = 1
  )

  expect_setequal(
    names(tvsm$validation_metrics_df),
    c("f1", "elastic_net_param_1", "reg_param_1")
  )
  expect_identical(nrow(tvsm$validation_metrics_df), 4L)
  summary_string <- capture.output(summary(tvsm)) %>%
    paste0(collapse = "\n")

  expect_match(
    summary_string,
    "0\\.75/0\\.25 train-validation split"
  )

  sub_models <- ml_sub_models(tvsm)
  expect_identical(length(sub_models), 4L)
  expect_identical(class(sub_models[[1]])[[1]], "ml_pipeline_model")
})


test_that("train validation split print methods", {
  sc <- testthat_spark_connection()
  lr <- ml_logistic_regression(sc, uid = "logistic")
  param_maps <- list(
    logistic = list(
      reg_param = c(0.1, 0.01),
      elastic_net_param = c(0.1, 0.2)
    )
  )
  evaluator <- ml_binary_classification_evaluator(sc, uid = "bineval")

  tvs1 <- ml_train_validation_split(sc, uid = "tvs")

  tvs2 <- ml_train_validation_split(
    sc,
    estimator = lr,
    estimator_param_maps = param_maps,
    evaluator = evaluator,
    uid = "tvs"
  )

  tvs3 <- ml_cross_validator(
    sc,
    estimator = lr,
    estimator_param_maps = param_maps,
    uid = "tvs"
  )

  expect_known_output(
    tvs1,
    output_file("print/tvs1.txt"),
    print = TRUE
  )

  expect_known_output(
    tvs2,
    output_file("print/tvs2.txt"),
    print = TRUE
  )

  expect_known_output(
    tvs3,
    output_file("print/tvs3.txt"),
    print = TRUE
  )
})

test_that("ml_cross_validator(tbl_spark) fits, prints, and reports metrics", {
  test_requires_version("2.3.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Species ~ Petal_Width, uid = "rf_cv") %>%
    ml_logistic_regression(uid = "lr_cv")

  # small grid + 2 folds keeps this quick enough to run during measurement, and
  # the tbl_spark entry point fits the model directly (exercises new_*_model,
  # print/summary, ml_validation_metrics and ml_sub_models for real).
  model <- ml_cross_validator(
    iris_tbl,
    estimator = pipeline,
    estimator_param_maps = list(lr_cv = list(reg_param = c(0, 0.1))),
    evaluator = ml_multiclass_classification_evaluator(sc),
    num_folds = 2,
    collect_sub_models = TRUE,
    seed = 1
  )

  expect_s3_class(model, "ml_cross_validator_model")
  expect_output(print(model), "Best Model")
  expect_output(summary(model), "Summary for")
  expect_equal(nrow(ml_validation_metrics(model)), 2)
  expect_length(ml_sub_models(model), 2)
})

test_clear_cache()
