context("random forest")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("thresholds parameter behaves as expected", {
  most_predicted_label <- function(x) x %>%
    count(prediction) %>%
    arrange(desc(n)) %>%
    pull(prediction) %>%
    first()

  rf_predictions <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(0, 1, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 0)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 1)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 1, 0)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 2)
})

test_that("error for thresholds with wrong length", {
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for col.sample.rate value out of range", {
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       col.sample.rate = 1.01),
    "'col.sample.rate' must be in \\(0, 1]"
  )
})

test_that("error for bad impurity specification", {
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "'impurity' must be 'gini' or 'entropy' for classification"
  )

  expect_error(
    iris_tbl %>%
      ml_random_forest(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "'impurity' must be 'variance' for regression"
  )
})
