skip_connection("ml_metrics")
skip_on_livy()

test_that("Multiclass metrics return expected results", {
  tbl_iris <- testthat_tbl("iris")

  model <- ml_random_forest(tbl_iris, "Species ~ .")

  tbl_predictions <- ml_predict(model, tbl_iris)

  metrics <- ml_metrics_multiclass(tbl_predictions)

  expect_is(metrics, "data.frame")
  expect_equal(dim(metrics), c(1, 3))
  expect_equal(pull(metrics, .estimate), 1)


  metrics <- ml_metrics_multiclass(tbl_predictions, metrics = c("recall", "precision"))

  expect_is(metrics, "data.frame")
  expect_equal(dim(metrics), c(2, 3))
  expect_equal(pull(metrics, .estimate), c(1, 1))

})

test_that("Binary metrics return expected results", {
  tbl_iris <- testthat_tbl("iris")

  prep_iris <- tbl_iris %>%
    mutate(is_setosa = ifelse(Species == "setosa", 1, 0))

  model <- ml_logistic_regression(prep_iris, "is_setosa ~ Sepal_Length")

  tbl_predictions <- ml_predict(model, prep_iris)

  metrics <- ml_metrics_binary(tbl_predictions)

  expect_is(metrics, "data.frame")
  expect_equal(dim(metrics), c(2, 3))
  expect_equal(pull(metrics, .estimate), c(0.959, 0.910), tolerance = 0.001)

})

test_that("Regression metrics return expected results", {
  tbl_iris <- testthat_tbl("iris")

  model <- ml_generalized_linear_regression(tbl_iris, "Sepal_Length ~ Sepal_Width + Petal_Length + Petal_Width ")

  tbl_predictions <- ml_predict(model, tbl_iris)

  metrics <- ml_metrics_regression(tbl_predictions, Sepal_Length)

  expect_is(metrics, "data.frame")
  expect_equal(dim(metrics), c(3, 3))
  expect_equal(pull(metrics, .estimate), c(0.310, 0.859, 0.252), tolerance = 0.001)

})
