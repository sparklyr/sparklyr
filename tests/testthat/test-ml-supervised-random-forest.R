context("ml supervised - random forest")

test_that("rf runs successfully when all args specified", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_random_forest(Species ~ Sepal_Width + Sepal_Length + Petal_Width, type = "classification",
                       feature_subset_strategy = "onethird", impurity = "entropy", max_bins = 16,
                       max_depth = 3, min_info_gain = 1e-5, min_instances_per_node = 2L,
                       num_trees = 25L, thresholds = c(1/2, 1/3, 1/4), seed = 42L),
    NA
  )
})

test_that("thresholds parameter behaves as expected", {
  skip_slow("takes too long to measure coverage")
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
    ml_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 0)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0, 1)) %>%
    ml_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 1)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 1, 0)) %>%
    ml_predict(iris_tbl)
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
  skip_slow("takes too long to measure coverage")
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
                              subsampling_rate = 1, feature_subset_strategy = "all",
                              num_trees = 1)
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression")

  expect_equal(rf %>%
                 ml_predict(iris_tbl) %>%
                 collect(),
               dt %>%
                 ml_predict(iris_tbl) %>%
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
                       cache_node_ids = TRUE,
                       checkpoint_interval = 5),
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
