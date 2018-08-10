context("ml feature string indexer + index to string")

test_that("ft_index_to_string() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_index_to_string)
})

test_that("ft_index_to_string() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    labels = c("rawr", "baz")
  )
  test_param_setting(sc, ft_index_to_string, test_args)
})

test_that("ft_string_indexer() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_string_indexer)
})

test_that("ft_string_indexer() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    handle_invalid = "keep",
    string_order_type = "frequencyAsc"
  )
  test_param_setting(sc, ft_string_indexer, test_args)
})

test_that("ft_string_indexer() works", {
  sc <- testthat_spark_connection()
  df <- dplyr::data_frame(string = c("foo", "bar", "foo", "foo"))
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)

  indexer <- ft_string_indexer(sc, "string", "indexed", dataset = df_tbl)
  expect_identical(ml_labels(indexer), c("foo", "bar"))

  # backwards compat
  my_env <- new.env(emptyenv())
  transformed <- df_tbl %>%
    ft_string_indexer("string", "indexed", params = my_env)
  expect_identical(my_env$labels, c("foo", "bar"))
})

test_that("ft_index_to_string() works", {

  sc <- testthat_spark_connection()
  df_tbl <- testthat_tbl("df")
  s1 <- df_tbl %>%
    ft_string_indexer("string", "indexed") %>%
    ft_index_to_string("indexed", "string2") %>%
    dplyr::pull(string2)

  expect_identical(s1, c("foo", "bar", "foo", "foo"))

  s2 <- df_tbl %>%
    ft_string_indexer("string", "indexed") %>%
    ft_index_to_string("indexed", "string2", c("wow", "cool")) %>%
    dplyr::pull(string2)

  expect_identical(s2, c("wow", "cool", "wow", "wow"))

  its <- ft_index_to_string(sc, "indexed", "string", labels = list("foo", "bar"))

  expect_equal(
    ml_params(its, list("input_col", "output_col", "labels")),
    list(input_col = "indexed",
         output_col = "string",
         labels = list("foo", "bar"))
  )
})

test_that("ft_string_indexer respects `string_order_type`", {
  test_requires_version("2.3.0", "string_order_type supported in Spark 2.3")
  sc <- testthat_spark_connection()
  df_tbl <- testthat_tbl("df")
  expect_identical(df_tbl %>%
    ft_string_indexer("string", "indexed", string_order_type = "alphabetAsc") %>%
    dplyr::pull(indexed),
    c(1, 0, 1, 1)
  )
})

test_that("ft_string_indexer_model works", {
  sc <- testthat_spark_connection()
  df_tbl <- testthat_tbl("df")
  expect_identical(
    ft_string_indexer_model(sc, "string", "indexed", labels = c("foo", "bar")) %>%
      ml_transform(df_tbl) %>%
      dplyr::pull(indexed),
    c(0, 1, 0, 0)
  )

  expect_identical(
    df_tbl %>%
      ft_string_indexer_model("string", "indexed", labels = c("bar", "foo")) %>%
      dplyr::pull(indexed),
    c(1, 0, 1, 1)
  )
})
