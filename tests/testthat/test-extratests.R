## Skip entire file is Spark is not installed

library(testthat)
library(parsnip)
library(dplyr)
library(modeldata)

# ------------------------------------------------------------------------------

data("hpc_data")

ctrl          <- control_parsnip(verbosity = 1, catch = FALSE)
caught_ctrl   <- control_parsnip(verbosity = 1, catch = TRUE)
quiet_ctrl    <- control_parsnip(verbosity = 0, catch = TRUE)

hpc <- hpc_data[1:150, c(2:5, 8)]

# ------------------------------------------------------------------------------

test_that('spark execution', {

  skip_if_not_installed("sparklyr")
  suppressPackageStartupMessages(library(sparklyr))

  sc <- testthat_spark_connection()

  skip_if(inherits(sc, "try-error"))

  hpc_bt_tr <- copy_to(sc, hpc[-(1:4),   ], "hpc_bt_tr", overwrite = TRUE)
  hpc_bt_te <- copy_to(sc, hpc[  1:4 , -1], "hpc_bt_te", overwrite = TRUE)

  # ----------------------------------------------------------------------------

  expect_error(
    spark_reg_fit <-
      fit(
        boost_tree(trees = 5, mode = "regression") %>%
          set_engine("spark", seed = 12),
        control = ctrl,
        compounds ~ .,
        data = hpc_bt_tr
      ),
    regexp = NA
  )

  # check for reproducibility and passing extra arguments
  expect_error(
    spark_reg_fit_dup <-
      fit(
        boost_tree(trees = 5, mode = "regression") %>%
          set_engine("spark", seed = 12),
        control = ctrl,
        compounds ~ .,
        data = hpc_bt_tr
      ),
    regexp = NA
  )

  expect_error(
    spark_reg_pred <- predict(spark_reg_fit, hpc_bt_te),
    regexp = NA
  )

  expect_error(
    spark_reg_pred_num <- parsnip:::predict_numeric.model_fit(spark_reg_fit, hpc_bt_te),
    regexp = NA
  )

  expect_error(
    spark_reg_dup <- predict(spark_reg_fit_dup, hpc_bt_te),
    regexp = NA
  )

  expect_error(
    spark_reg_num_dup <- parsnip:::predict_numeric.model_fit(spark_reg_fit_dup, hpc_bt_te),
    regexp = NA
  )

  expect_equal(colnames(spark_reg_pred), "pred")

  expect_equal(
    as.data.frame(spark_reg_pred)$pred,
    as.data.frame(spark_reg_dup)$pred
  )
  expect_equal(
    as.data.frame(spark_reg_pred_num)$pred,
    as.data.frame(spark_reg_num_dup)$pred
  )


  # ----------------------------------------------------------------------------

  # same for classification

  churn_bt_tr <- copy_to(sc, wa_churn[ 5:100,   ], "churn_bt_tr", overwrite = TRUE)
  churn_bt_te <- copy_to(sc, wa_churn[   1:4, -1], "churn_bt_te", overwrite = TRUE)

  # ----------------------------------------------------------------------------

  expect_error(
    spark_class_fit <-
      fit(
        boost_tree(trees = 5, mode = "classification") %>%
          set_engine("spark", seed = 12),
        control = ctrl,
        churn ~ .,
        data = churn_bt_tr
      ),
    regexp = NA
  )

  # check for reproducibility and passing extra arguments
  expect_error(
    spark_class_fit_dup <-
      fit(
        boost_tree(trees = 5, mode = "classification") %>%
          set_engine("spark", seed = 12),
        control = ctrl,
        churn ~ .,
        data = churn_bt_tr
      ),
    regexp = NA
  )

  expect_error(
    spark_class_pred <- predict(spark_class_fit, churn_bt_te),
    regexp = NA
  )

  expect_error(
    spark_class_pred_class <- parsnip:::predict_class.model_fit(spark_class_fit, churn_bt_te),
    regexp = NA
  )

  expect_error(
    spark_class_dup <- predict(spark_class_fit_dup, churn_bt_te),
    regexp = NA
  )

  expect_error(
    spark_class_dup_class <- parsnip:::predict_class.model_fit(spark_class_fit_dup, churn_bt_te),
    regexp = NA
  )

  expect_equal(colnames(spark_class_pred), "pred_class")

  expect_equal(
    as.data.frame(spark_class_pred)$pred_class,
    as.data.frame(spark_class_dup)$pred_class
  )
  expect_equal(
    as.data.frame(spark_class_pred_class)$pred_class,
    as.data.frame(spark_class_dup_class)$pred_class
  )


  expect_error(
    spark_class_prob <- predict(spark_class_fit, churn_bt_te, type = "prob"),
    regexp = NA
  )

  expect_error(
    spark_class_prob_classprob <- parsnip:::predict_classprob.model_fit(spark_class_fit, churn_bt_te),
    regexp = NA
  )

  expect_error(
    spark_class_dup <- predict(spark_class_fit_dup, churn_bt_te, type = "prob"),
    regexp = NA
  )

  expect_error(
    spark_class_dup_classprob <- parsnip:::predict_classprob.model_fit(spark_class_fit_dup, churn_bt_te),
    regexp = NA
  )

  expect_equal(colnames(spark_class_prob), c("pred_No", "pred_Yes"))

  expect_equal(
    as.data.frame(spark_class_prob),
    as.data.frame(spark_class_dup),
    ignore_attr = TRUE
  )
  expect_equal(
    as.data.frame(spark_class_prob_classprob),
    as.data.frame(spark_class_dup_classprob)
  )

  spark_disconnect_all()
})
