context("ml feature string indexer")

sc <- testthat_spark_connection()

df <- dplyr::data_frame(string = c("foo", "bar", "foo", "foo"))
df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)

test_that("ft_string_indexer() works", {
  indexer <- ft_string_indexer(sc, "string", "indexed", dataset = df_tbl)

  expect_identical(ml_labels(indexer), c("foo", "bar"))

  # backwards compat
  my_env <- new.env(emptyenv())
  transformed <- df_tbl %>%
    ft_string_indexer("string", "indexed", params = my_env)
  expect_identical(my_env$labels, c("foo", "bar"))
})

test_that("ft_index_to_string() works", {


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
