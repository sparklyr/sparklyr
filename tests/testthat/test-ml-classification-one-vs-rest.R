context("ml classification - one vs rest")

test_that("ml_one_vs_rest() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_one_vs_rest)
})

test_that("ml_one_vs_rest() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    features_col = "wefaef",
    label_col = "weijfw",
    prediction_col = "weifjwifj"
  )
  test_param_setting(sc, ml_one_vs_rest, test_args)
})

test_that("ml_one_vs_rest with two classes agrees with logistic regression", {
  sc <- testthat_spark_connection()
  iris_tbl2 <- testthat_tbl("iris") %>%
    mutate(is_versicolor = ifelse(
      Species == "versicolor", "versicolor", "other")) %>%
    select(-Species)

  lr_model <- ml_logistic_regression(iris_tbl2, formula = is_versicolor ~ .)
  lr <- ml_logistic_regression(sc)
  ovr_model <- ml_one_vs_rest(iris_tbl2, is_versicolor ~ ., classifier = lr)

  expect_equal(
    sdf_predict(ovr_model, iris_tbl2) %>% pull(predicted_label),
    sdf_predict(lr_model, iris_tbl2) %>% pull(predicted_label)
  )
})

test_that("ml_one_vs_rest fits the right number of estimators", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  lr <- ml_logistic_regression(sc)

  ovr_model <- ml_one_vs_rest(iris_tbl, Species ~ ., classifier = lr)

  expect_equal(length(ovr_model$model$models), 3)
})

test_that("ml_one_vs_rest() errors when not given classifier", {
  sc <- testthat_spark_connection()
  expect_error(
    ml_one_vs_rest(sc, classifier = ml_random_forest_regressor(sc)),
    "`classifier` must be an `ml_classifier`\\."
  )
})
