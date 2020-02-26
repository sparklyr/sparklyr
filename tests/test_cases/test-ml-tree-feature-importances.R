context("ml tree feature importances")

test_that("ml_tree_feature_importance() works properly", {
  test_requires_version("2.0.0", "feature importances not available prior to spark 2.0")
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

  rf_importance <- ml_tree_feature_importance(rf)
  dt_importance <- ml_tree_feature_importance(dt)
  expect_equal(colnames(rf_importance), c("feature", "importance"))
  expect_equal(nrow(rf_importance), 3)
  expect_equal(rf_importance, dt_importance)
})

test_that("ml_tree_feature_importance() works for decision tree classification", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  dt <- iris_tbl %>%
    ml_decision_tree(
      Species ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width,
      type = "classification"
    )
  expect_identical(
    ml_tree_feature_importance(dt) %>% names(),
    c("feature", "importance")
  )
})

test_that("ml_feature_importances work properly (#1436)", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")
  feature_cols <-
    c("mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "gear", "carb")

  vector_assembler <-
    ft_vector_assembler(
      sc,
      input_cols = feature_cols,
      output_col = "features")

  estimator <-
    ml_random_forest_classifier(
      sc,
      label_col = "am")

  pipeline <- ml_pipeline(vector_assembler, estimator)

  pipeline_model <- pipeline %>%
    ml_fit(mtcars_tbl)

  importances1 <- data.frame(
    feature = feature_cols,
    importance = ml_feature_importances(ml_stage(pipeline_model, 2)),
    stringsAsFactors = FALSE
  ) %>%
    arrange(desc(importance))

  model <- ml_random_forest_classifier(
    mtcars_tbl,
    formula = am ~ mpg + cyl + disp + hp + drat + wt + qsec + vs + gear + carb
  )

  importances2 <- ml_tree_feature_importance(model)

  expect_equal(importances1, importances2)
})
