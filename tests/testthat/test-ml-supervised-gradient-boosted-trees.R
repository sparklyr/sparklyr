skip_connection("ml-supervised-gradient-boosted-trees")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

sc <- testthat_spark_connection()
iris_tbl <- testthat_tbl("iris")

sans_setosa <- iris_tbl %>%
  filter(Species != "setosa")

test_that("gbt runs successfully when all args specified", {
  model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "classification",
      max_bins = 16,
      max_depth = 3, min_info_gain = 1e-5, min_instances_per_node = 2,
      seed = 42
    )
  expect_equal(class(model)[1], "ml_model_gbt_classification")
})

test_that("thresholds parameter behaves as expected", {
  test_requires_version("2.2.0", "thresholds not supported for GBT for Spark <2.2.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  most_predicted_label <- function(x) {
    expect_warning_on_arrow(
      x %>%
        collect() %>%
        count(prediction) %>%
        arrange(desc(n)) %>%
        pull(prediction) %>%
        first()
    )
  }

  gbt_predictions <-sans_setosa  %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(0, 1)
    ) %>%
    ml_predict(sans_setosa)

  expect_equal(most_predicted_label(gbt_predictions), 0)

  gbt_predictions <- sans_setosa %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(1, 0)
    ) %>%
    ml_predict(sans_setosa)

  expect_equal(most_predicted_label(gbt_predictions), 1)
})

test_that("informative error when using Spark version that doesn't support thresholds", {
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= "2.2.0") skip("not applicable, threshold is supported")

  expect_error(
    sans_setosa %>%
      ml_gradient_boosted_trees(Species ~ Sepal_Width,
        type = "classification",
        thresholds = c(0, 1)
      ),
    "`thresholds` is only supported for GBT in Spark 2.2.0\\+"
  )
})

test_that("one-tree ensemble agrees with ml_decision_tree()", {
  sc <- testthat_spark_connection()
  gbt <- iris_tbl %>%
    ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression",
      subsampling_rate = 1,
      max_iter = 1
    )
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression"
    )

  expect_equal(
    gbt %>%
      ml_predict(iris_tbl) %>%
      collect(),
    dt %>%
      ml_predict(iris_tbl) %>%
      collect()
  )
})

test_that("checkpointing works for gbt", {
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_gradient_boosted_trees(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
        type = "regression",
        cache_node_ids = TRUE,
        checkpoint_interval = 5
      ),
    NA
  )
})

test_that("ml_gradient_boosted_trees() supports response-features syntax", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_gradient_boosted_trees(iris_tbl,
      response = "Sepal_Length",
      features = c("Sepal_Width", "Petal_Length")
    ),
    NA
  )
})
