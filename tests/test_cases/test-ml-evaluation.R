context("ml evaluator")

test_that("basic binary classification evaluation works", {
  sc <- testthat_spark_connection()
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
  sc <- testthat_spark_connection()
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

test_that("ml evaluator print methods work", {
  sc <- testthat_spark_connection()
  skip_on_spark_master()

  expect_known_output(
    ml_binary_classification_evaluator(sc, uid = "foo"),
    output_file("print/binary-classification-evaluator.txt"),
    print = TRUE
  )

  expect_known_output(
   ml_multiclass_classification_evaluator(sc, uid = "foo"),
    output_file("print/multiclass-classification-evaluator.txt"),
    print = TRUE
  )

  expect_known_output(
    ml_regression_evaluator(sc, uid = "foo"),
    output_file("print/regression-evaluator.txt"),
    print = TRUE
  )
})
