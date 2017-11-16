context("ml evaluator")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
#
# test_that("basic classification evaluation works", {
#   setosa_tbl <- iris_tbl %>% dplyr::mutate(Is_Setosa = ifelse(Species == "setosa", 1, 0))
#
#   ml_fit <- ml_logistic_regression(setosa_tbl, Is_Setosa ~ .)
#   ml_pred <- sdf_predict(ml_fit, setosa_tbl)
#   ml_pred <- dplyr::mutate(ml_pred, actual = ifelse(Species == "setosa", 1, 0))
#   accuracy <- ml_classification_eval(ml_pred, "actual", "prediction", "accuracy")
#
#   expect_equal(accuracy, 1)
# })

test_that("basic binary classification evaluation works", {
  df <- data.frame(label = c(1, 1, 0, 0), features1 = c(1, 1, 0, 0))
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)
  model <- df_tbl %>%
    ft_vector_assembler("features1","features") %>%
    ml_logistic_regression()
  auc <- ml_binary_classification_evaluator(
    model %>%
      ml_predict(ft_vector_assembler(df_tbl, "features1","features")),
    label_col = "label", raw_prediction_col = "rawPrediction")
  expect_equal(auc, 1)
})

test_that("basic regression evaluation works", {
  df <- data.frame(label = c(1.2, 4.5, 6.7),
                   prediction = c(3, 5, 7))
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)
  mse_r <- df %>%
    dplyr::summarize(mse = sum((label-prediction) ^ 2) / 3) %>%
    dplyr::pull(mse)

  mse_s <- ml_regression_evaluator(
    df_tbl, label_col = "label", prediction_col = "prediction", metric_name = "mse")

  expect_equal(mse_r, mse_s)
})
