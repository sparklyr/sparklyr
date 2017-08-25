context("decision tree")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("decision tree runs successfully when all args specified", {
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
                       type = "classification",
                       impurity = "entropy", max.bins = 16L,
                       max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                       thresholds = c(1/2, 1/3, 1/4), seed = 42L),
    NA
  )
})

test_that("thresholds parameter behaves as expected", {
  test_requires("dplyr")
  most_predicted_label <- function(x) x %>%
    count(prediction) %>%
    arrange(desc(n)) %>%
    pull(prediction) %>%
    first()

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(0, 1, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 0)

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 1)

  dt_predictions <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 1, 0)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(dt_predictions), 2)
})

test_that("error for thresholds with wrong length", {
  if (spark_version(sc) < "2.1.0") skip("threshold length checking implemented in 2.1.0")
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for bad impurity specification", {
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "'impurity' must be 'gini' or 'entropy' for classification"
  )

  expect_error(
    iris_tbl %>%
      ml_decision_tree(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "'impurity' must be 'variance' for regression"
  )
})
