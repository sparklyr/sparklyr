context("ml classification - one vs rest")

sc <- testthat_spark_connection()

test_that("ml_one_vs_rest param setting", {
  lr <- ml_logistic_regression(sc)
  args <- list(
    x = sc, classifier = lr,
    label_col = "col", features_col = "fcol", prediction_col = "pcol"
  )
  ovr <- do.call(ml_one_vs_rest, args)
  expect_equal(ml_params(ovr, names(args)[-1:-2]), args[-1:-2])
})

test_that("ml_one_vs_rest() default params are correct", {
  lr <- ml_logistic_regression(sc)
  predictor <- ml_pipeline(sc) %>%
    ml_one_vs_rest(classifier = lr) %>%
    ml_stage(1)

  args <- get_default_args(ml_one_vs_rest,
                           c("x", "uid", "...", "classifier"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_one_vs_rest with two classes agrees with logistic regression", {
  test_requires("dplyr")
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
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  lr <- ml_logistic_regression(sc)

  ovr_model <- ml_one_vs_rest(iris_tbl, Species ~ ., classifier = lr)

  expect_equal(length(ovr_model$model$models), 3)
})
