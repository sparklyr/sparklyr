context("gradient boosted trees")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("gbt runs successfully when all args specified", {
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
                       type = "classification",
                       impurity = "entropy", max.bins = 16L,
                       max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                       seed = 42L),
    NA
  )
})

test_that("thresholds parameter behaves as expected", {
  if (spark_version(sc) < "2.2.0") skip("thresholds not supported for GBT for Spark <2.2.0")
  test_requires("dplyr")
  most_predicted_label <- function(x) x %>%
    count(prediction) %>%
    arrange(desc(n)) %>%
    pull(prediction) %>%
    first()

  gbt_predictions <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(0, 1)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(gbt_predictions), 0)

  gbt_predictions <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                     thresholds = c(1, 0)) %>%
    sdf_predict(iris_tbl)
  expect_equal(most_predicted_label(gbt_predictions), 1)
})

test_that("informative error when using Spark version that doesn't support thresholds", {
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                                thresholds = c(0, 1)),
    "thresholds is only supported for GBT in Spark 2.2.0\\+"
  )
})

test_that("error for thresholds with wrong length", {
  if (spark_version(sc) < "2.2.0") skip("thresholds not supported for Spark <2.2.0")
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                       thresholds = c(0, 1, 1)),
    "non-matching numClasses and thresholds.length"
  )
})

test_that("error for bad impurity specification", {
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                       impurity = "variance"),
    "'impurity' must be 'gini' or 'entropy' for classification"
  )

  expect_error(
    iris_tbl %>%
      ml_gradient_boosted_trees(Sepal_Length ~ Sepal_Width, type = "regression",
                       impurity = "gini"),
    "'impurity' must be 'variance' for regression"
  )
})

test_that("one-tree ensemble agrees with ml_decision_tree()", {
  gbt <- iris_tbl %>%
    ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression",
                     sample.rate = 1,
                     num.trees = 1L)
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression")

  expect_equal(gbt %>%
                 sdf_predict(iris_tbl) %>%
                 collect(),
               dt %>%
                 sdf_predict(iris_tbl) %>%
                 collect())
})

test_that("checkpointing works for gbt", {
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                                type = "regression",
                                cache.node.ids = TRUE,
                                checkpoint.interval = 5L),
    NA
  )
})
