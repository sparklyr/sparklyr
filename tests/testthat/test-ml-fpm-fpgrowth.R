context("ml fpm fpgrowth")

sc <- testthat_spark_connection()

test_that("ml_fpgrowth() works properly", {
  test_requires_version("2.2.0", "fpgrowth requires spark 2.2.0+")
  test_requires("dplyr")

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
