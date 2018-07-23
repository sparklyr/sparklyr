context("ml supervised - random forest")

test_that("rf runs successfully when all args specified", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) >= "2.0.0") skip("not applicable to 2.0+")
  expect_message(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                       col.sample.rate = 1/3),
    "Using feature subsetting strategy: onethird"
  )

  expect_message(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width + Petal_Length, type = "classification",
                       col.sample.rate = 1/2),
    "Using feature subsetting strategy: log2"
  )
})

test_that("col.sample.rate argument is respected", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.0") skip("not applicable to <2.0")
  rf <- ml_random_forest(iris_tbl, Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                         col.sample.rate = 0.001)

  expect_equal(ml_param(rf$model, "feature_subset_strategy"),
               "0.001")
})

test_that("thresholds parameter behaves as expected", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.1.0") skip("threshold length checking implemented in 2.1.0")
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for col.sample.rate value out of range", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       col.sample.rate = 1.01),
    "`col.sample.rate` must be in \\(0, 1]\\."
  )
})

test_that("error for bad impurity specification", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "`impurity` must be \"gini\" or \"entropy\" for classification\\."
  )

  expect_error(
    iris_tbl %>%
      ml_random_forest(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "`impurity` must be \"variance\" for regression\\."
  )
})

test_that("random seed setting works", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  model_string <- function(x) spark_jobj(x$model) %>%
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_random_forest(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                       type = "regression",
                       cache.node.ids = TRUE,
                       checkpoint.interval = 5L),
  NA)
})

test_that("ml_random_forest() provides informative error for bad response_col", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(iris_tbl, Sepal.Length ~ Sepal.Width),
    "`Sepal.Length` is not a column in the input dataset\\.")
})

test_that("residuals() call on ml_model_random_forest_regression errors", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(iris_tbl, Sepal_Length ~ Sepal_Width) %>% residuals(),
    "'residuals\\(\\)' not supported for ml_model_random_forest_regression"
  )
})

test_that("ml_random_forest() supports response-features syntax", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(iris_tbl,
                     response = 'Sepal_Length',
                     features = c('Sepal_Width', 'Petal_Length')),
    NA
  )
})
