context("ml feature imputer")

sc <- testthat_spark_connection()

test_that("ft_imputer() works properly", {
  test_requires_version("2.2.0", "imputer requires Spark 2.2.0+")
  df <- data.frame(id = 1:5, V1 = c(1, 2, NA, 4, 5))
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)
  imputed_tbl <- df_tbl %>%
    ft_imputer("V1", "imputed")
  expect_equal(dplyr::pull(imputed_tbl, imputed)[[3]], 3)
})
