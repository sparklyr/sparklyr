skip_connection("feature-one-hot-encoder-estimator")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_one_hot_encoder_estimator() default params", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_one_hot_encoder_estimator)
})

test_that("ft_one_hot_encoder_estimator() param setting", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "foo1"),
    output_cols = c("bar", "bar1"),
    drop_last = FALSE
  )
  test_param_setting(sc, ft_one_hot_encoder_estimator, test_args)
})

test_that("ft_one_hot_encoder_estimator() works", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_equal(
    iris_tbl %>%
      ft_string_indexer("Species", "indexed") %>%
      ft_one_hot_encoder_estimator("indexed", "encoded") %>%
      pull(encoded) %>% unique(),
    list(c(0, 0), c(1, 0), c(0, 1))
  )
})

test_clear_cache()

