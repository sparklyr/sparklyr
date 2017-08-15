context("random forest")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("rf runs successfully when all args specified", {
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                       col.sample.rate = 1/3, impurity = "entropy", max.bins = 16L,
                       max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                       num.trees = 25L, thresholds = c(1/2, 1/3, 1/4), seed = 42L),
    NA
  )
})

test_that("col.sample.rate maps to correct strategy", {
  if (spark_version(sc) >= "2.0.0") skip("not applicable to 2.0+")
  expect_message(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                       col.sample.rate = 1/3),
    "Using feature subsetting strategy: onethird"
  )

  expect_message(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                       col.sample.rate = 0.001),
    "Using feature subsetting strategy: log2"
  )
})

test_that("thresholds parameter behaves as expected", {
  test_requires("dplyr")
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
  if (spark_version(sc) < "2.1.0") skip("threshold length checking implemented in 2.1.0")
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

test_that("random seed setting works", {
  model_string <- function(x) x$.model %>%
    invoke("toDebugString") %>%
    strsplit("\n") %>%
    unlist() %>%
    tail(-1)

  m1 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     seed = 42L)

  m2 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     seed = 42L)

  expect_equal(model_string(m1), model_string(m2))
})

test_that("one-tree forest agrees with ml_decision_tree()", {
  rf <- iris_tbl %>%
    ml_random_forest(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                              type = "regression",
                              sample.rate = 1, col.sample.rate = 1,
                              num.trees = 1L)
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression")

  expect_equal(rf %>%
                 sdf_predict(iris_tbl) %>%
                 collect(),
               dt %>%
                 sdf_predict(iris_tbl) %>%
                 collect())
})

test_that("checkpointing works for rf", {
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_random_forest(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                       type = "regression",
                       cache.node.ids = TRUE,
                       checkpoint.interval = 5L),
  NA)
})
