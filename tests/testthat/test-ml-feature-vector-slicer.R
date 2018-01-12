context("ml feature vector slicer")

sc <- testthat_spark_connection()

test_that("ft_vector_slicer works", {
  df <- data.frame(
    V1 = 1,
    V2 = 2,
    V3 = 3
  )
  sliced <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(input_cols = paste0("V", 1:3), output_col = "vector") %>%
    ft_vector_slicer("vector", "sliced", 0:1) %>%
    dplyr::pull(sliced)
  expect_identical(sliced, list(c(1, 2)))
})
