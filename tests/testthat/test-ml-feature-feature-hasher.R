skip_connection("ml-feature-feature-hasher")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_feature_hasher() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "bar"),
    output_col = "raw",
    num_features = 2^10,
    categorical_cols = "foo"
  )
  test_param_setting(sc, ft_feature_hasher, test_args)
})

test_that("ft_feature_hasher() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.3.0", "ft_feature_hasher() requires spark 2.3+")
  df <- tribble(
    ~real, ~bool, ~stringNum, ~string,
    2.0, TRUE, "1", "foo",
    3.0, FALSE, "2", "bar"
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    f_h <- df_tbl %>%
      ft_feature_hasher(
        input_cols = c("real", "bool", "stringNum", "string"),
        output_col = "features",
        num_features = 2^5
      ) %>%
      pull(features) %>%
      first() %>%
      length()
  )

  expect_identical(f_h, 32L)
})

test_clear_cache()

