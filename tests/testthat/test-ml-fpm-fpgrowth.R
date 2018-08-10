context("ml fpm fpgrowth")

test_that("ml_fpgrowth() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_fpgrowth)
})

test_that("ml_fpgrowth() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    items_col = "wefwef",
    min_confidence = 0.7,
    min_support = 0.4,
    prediction_col = "waef"
  )
  test_param_setting(sc, ml_fpgrowth, test_args)
})

test_that("ml_fpgrowth() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.2.0", "fpgrowth requires spark 2.2.0+")

  df <- data.frame(items = c("1 2 5", "1 2 3 5", "1 2"))
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    mutate(items = split(items, " "))
  fp_model <- df_tbl %>%
    ml_fpgrowth(min_support = 0.5, min_confidence = 0.6)
  expect_equal(
    ml_freq_itemsets(fp_model) %>%
      sdf_nrow(),
    7)

  expect_equal(
    ml_association_rules(fp_model) %>%
      sdf_nrow(),
    9)

  expect_identical(
    fp_model %>%
      ml_transform(df_tbl) %>%
      pull(prediction),
    list(list(), list(), list("5"))
  )
})
