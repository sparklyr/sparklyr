skip_connection("ml-feature-one-hot-encoder")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
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
    num_cols <- if (spark_version(sc) >= "3.0.0") 2 else 1
    indexed_cols <- paste0("indexed", seq(num_cols))
    encoded_cols <- paste0("encoded", seq(num_cols))

    encoded_tbl <- iris_tbl %>%
      mutate(Species1 = Species, Species2 = Species) %>%
      ft_string_indexer("Species1", "indexed1", string_order_type = "alphabetDesc") %>%
      ft_string_indexer("Species2", "indexed2", string_order_type = "alphabetDesc") %>%
      ft_one_hot_encoder(indexed_cols, encoded_cols) %>%
      compute()
    for (x in encoded_cols) {
      expect_warning_on_arrow(
        expect_setequal(
          encoded_tbl %>% pull(!!x) %>% unique(), list(c(0, 0), c(0, 1), c(1, 0))
        )
      )
    }
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

test_that("ft_one_hot_encoder() works with ml pipeline", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.3.0") {
    pipeline <- ml_pipeline(sc) %>%
      ft_string_indexer("Species", "indexed") %>%
      ft_one_hot_encoder("indexed", "encoded")
  } else {
    pipeline <- ml_pipeline(sc) %>%
      ft_string_indexer("Species", "indexed", string_order_type = "alphabetDesc") %>%
      ft_one_hot_encoder("indexed", "encoded")
  }

  expect_warning_on_arrow(
    f_o <- pipeline %>%
      ml_fit_and_transform(iris_tbl) %>%
      pull(encoded) %>%
      unique()
  )

  expect_setequal(
    f_o,
    list(c(0, 0), c(1, 0), c(0, 1))
  )
})
