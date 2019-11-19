context("ml feature one hot encoder")

test_that("ft_one_hot_encoder() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_one_hot_encoder)
})

test_that("ft_one_hot_encoder() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    input_cols = c("foo", "foo1"),
    output_cols = c("bar", "bar1"),
    drop_last = FALSE
  )
  test_param_setting(sc, ft_one_hot_encoder, test_args)
})

test_that("ft_one_hot_encoder() works", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.3.0") {
    expect_equal(
      iris_tbl %>%
        ft_string_indexer("Species", "indexed") %>%
        ft_one_hot_encoder("indexed", "encoded") %>%
        pull(encoded) %>% unique(),
      list(c(0, 0), c(1, 0), c(0, 1))
    )
  } else {
    expect_equal(
      iris_tbl %>%
        ft_string_indexer("Species", "indexed", string_order_type = "alphabetDesc") %>%
        ft_one_hot_encoder("indexed", "encoded") %>%
        pull(encoded) %>% unique(),
      list(c(0, 0), c(0, 1), c(1, 0))
    )
  }

})
