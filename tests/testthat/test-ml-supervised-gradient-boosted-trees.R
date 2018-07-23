context("ml supervised - gradient boosted trees")

test_that("gbt runs successfully when all args specified", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
                              type = "classification",
                              max.bins = 16L,
                              max.depth = 3L, min.info.gain = 1e-5, min.rows = 2L,
                              seed = 42L)
  expect_equal(class(model)[1], "ml_model_gbt_classification")
})

test_that("thresholds parameter behaves as expected", {
  test_requires_version("2.2.0", "thresholds not supported for GBT for Spark <2.2.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= "2.2.0") skip("not applicable, threshold is supported")
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      filter(Species != "setosa") %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width, type = "classification",
                                thresholds = c(0, 1)),
    "`thresholds` is only supported for GBT in Spark 2.2.0\\+"
  )
})

test_that("one-tree ensemble agrees with ml_decision_tree()", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
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

test_that('ml_gradient_boosted_trees() supports response-features syntax', {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  ml_gradient_boosted_trees(iris_tbl,
                            response = 'Sepal_Length',
                            features = c('Sepal_Width', 'Petal_Length'))
})
