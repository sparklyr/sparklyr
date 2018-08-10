context("ml feature imputer")

test_that("ft_imputer() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_imputer)
})

test_that("ft_imputer() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo1", "foo2"),
    output_cols = c("bar1", "bar2"),
    missing_value = 24,
    strategy = "median"
  )
  test_param_setting(sc, ft_imputer, test_args)
})

test_that("ft_imputer() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.2.0", "imputer requires Spark 2.2.0+")
  df <- data.frame(id = 1:5, V1 = c(1, 2, NA, 4, 5))
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)
  imputed_tbl <- df_tbl %>%
    ft_imputer("V1", "imputed")
  expect_equal(pull(imputed_tbl, imputed)[[3]], 3)
})
