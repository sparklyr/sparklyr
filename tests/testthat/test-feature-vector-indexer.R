skip_connection("feature-vector-indexer")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_vector_indexer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_vector_indexer)
})

test_that("ft_vector_indexer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    max_categories = 15
  )
  test_param_setting(sc, ft_vector_indexer, test_args)
})

test_that("ft_vector_indexer() works properly", {
  sc <- testthat_spark_connection()
  sample_data_path <- get_test_data_path("sample_libsvm_data.txt")
  sample_data <- spark_read_libsvm(sc, "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  indexer <- ft_vector_indexer(sc,
    input_col = "features", output_col = "indexed",
    max_categories = 10
  ) %>%
    ml_fit(sample_data)

  expect_warning_on_arrow(
    expect_identical(
      indexer %>%
        ml_transform(sample_data) %>%
        head(1) %>%
        pull(indexed) %>%
        unlist() %>%
        length(),
      692L
    )
  )

  expect_warning_on_arrow(
    expect_identical(
      sample_data %>%
        ft_vector_indexer("features", "indexed", max_categories = 10) %>%
        head(1) %>%
        pull(indexed) %>%
        unlist() %>%
        length(),
      692L
    )
  )
})

test_clear_cache()

