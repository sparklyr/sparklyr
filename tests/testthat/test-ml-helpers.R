context("ml helpers")

sc <- testthat_spark_connection()

test_that("ml_feature_importances work properly (#1436)", {
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
    dplyr::arrange(desc(importance))

  model <- ml_random_forest_classifier(
    mtcars_tbl,
    formula = am ~ mpg + cyl + disp + hp + drat + wt + qsec + vs + gear + carb
  )

  importances2 <- ml_tree_feature_importance(model)

  expect_equal(importances1, importances2)
})
