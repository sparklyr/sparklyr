context("ml supervised - decision tree")

test_that("decision tree runs successfully when all args specified", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

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
  test_requires_version("2.1.0", "threshold length checking implemented in 2.1.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for bad impurity specification", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_decision_tree(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "`impurity` must be \"gini\" or \"entropy\" for classification\\."
  )

  expect_error(
    iris_tbl %>%
      ml_decision_tree(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "`impurity` must be \"variance\" for regression\\."
  )
})

test_that("ml_decision_tree print outputs are correct", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_output_file(
    print(ml_decision_tree(iris_tbl, Species ~ Petal_Length + Sepal_Width,
                           seed = 24, uid = "dt1")),
    output_file("print/decision-tree.txt")
  )

  expect_output_file(
    print(ml_decision_tree_classifier(iris_tbl, Species ~ Petal_Length + Sepal_Width,
                                      seed = 54, uid = "dt2")),
    output_file("print/decision_tree_classifier.txt")
  )
})

test_that("ml_decision_tree() supports response-features syntax (#1302)", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_decision_tree(iris_tbl,
                     response = 'Sepal_Length',
                     features = c('Sepal_Width', 'Petal_Length')),
    NA
  )
})
