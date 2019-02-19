context("ml tuning train validation split")

test_that("ml_train_validation_split() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_train_validation_split)
})

test_that("ml_train_validation_split() param setting", {
  test_requires_latest_spark()
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
    iris_tbl, estimator = pipeline, estimator_param_maps = grid,
    evaluator = ml_multiclass_classification_evaluator(sc),
    collect_sub_models = TRUE,
    seed = 1)

  expect_identical(names(tvsm$validation_metrics_df),
                   c("f1", "elastic_net_param_1", "reg_param_1"))
  expect_identical(nrow(tvsm$validation_metrics_df), 4L)
  summary_string <- capture.output(summary(tvsm)) %>%
    paste0(collapse = "\n")

  expect_match(summary_string,
               "0\\.75/0\\.25 train-validation split")

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
