context("ml feature vector indexer")

sc <- testthat_spark_connection()

test_that("ft_vector_indexer() works properly", {
  sample_data_path <- dir(getwd(), recursive = TRUE, pattern = "sample_libsvm_data.txt", full.names = TRUE)
  sample_data <- spark_read_libsvm(sc, "sample_data",
                                   sample_data_path, overwrite = TRUE)
  indexer <- ft_vector_indexer(sc, input_col = "features", output_col = "indexed",
                               max_categories = 10, dataset = sample_data)
  expect_identical(indexer %>%
    ml_transform(sample_data) %>%
    head(1) %>%
    dplyr::pull(indexed) %>%
    unlist() %>%
    length(),
  692L)

  expect_identical(sample_data %>%
                     ft_vector_indexer("features", "indexed", max_categories = 10) %>%
                     head(1) %>%
                     dplyr::pull(indexed) %>%
                     unlist() %>%
                     length(),
                   692L)
})
