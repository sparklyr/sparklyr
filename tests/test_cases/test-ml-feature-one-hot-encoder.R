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

test_that("ft_one_hot_encoder() with multiple columns", {
  sc <- testthat_spark_connection()
  df <- tibble(
    id = 0:5L,
    input1 = c(0, 1, 2, 0, 0, 2),
    input2 = c(2, 3, 0, 1, 0, 2)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  if (spark_version(sc) < "3.0.0") {
    expect_error(df_tbl %>%
                   ft_one_hot_encoder(c("input1", "input2"), c("output1", "output2")))
  }
  else {
    expect_identical(
      df_tbl %>%
        ft_one_hot_encoder(c("input1", "input2"), c("output1", "output2")) %>%
        colnames(),
      c("id", "input1", "input2", "output1", "output2")
    )
  }
})
