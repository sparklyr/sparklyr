skip_connection("ml-feature-vector-slicer")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_vector_slicer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    indices = 1:5
  )
  test_param_setting(sc, ft_vector_slicer, test_args)
})

test_that("ft_vector_slicer works", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    V1 = 1,
    V2 = 2,
    V3 = 3
  )

  expect_warning_on_arrow(
    sliced <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
      ft_vector_assembler(input_cols = paste0("V", 1:3), output_col = "vector") %>%
      ft_vector_slicer("vector", "sliced", 0:1) %>%
      pull(sliced)
  )

  expect_identical(sliced, list(c(1, 2)))
})

test_clear_cache()

