context("classification eval")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("basic classification evaluation works", {
  setosa_tbl <- iris_tbl %>% mutate(Is_Setosa = ifelse(Species == "setosa", 1, 0))

  ml_fit <- ml_logistic_regression(setosa_tbl, Is_Setosa ~ .)
  ml_pred <- sdf_predict(ml_fit, setosa_tbl)
  ml_pred <- mutate(ml_pred, actual = ifelse(Species == "setosa", 1, 0))
  accuracy <- ml_classification_eval(ml_pred, "actual", "prediction", "accuracy")

  expect_equal(accuracy, 1)
})
