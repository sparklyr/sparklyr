context("ml feature - feature hasher")

sc <- testthat_spark_connection()

test_that("ft_feature_hasher() works", {
  test_requires_version("2.3.0", "ft_feature_hasher() requires spark 2.3+")
  df <- tibble::tribble(
    ~real, ~bool, ~stringNum, ~string,
    2.0,   TRUE,  "1",        "foo",
    3.0,   FALSE, "2",        "bar"
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)
  expect_identical(
    df_tbl %>%
      ft_feature_hasher(input_cols = c("real", "bool", "stringNum", "string"),
                        output_col = "features",
                        num_features = 2^5) %>%
      dplyr::pull(features) %>%
      dplyr::first() %>%
      length(),
    32L
  )
})
