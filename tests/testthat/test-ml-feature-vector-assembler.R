context("ml feature vector assembler")

test_that("ft_vector_assembler() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "baz"),
    output_col = "bar"
  )
  test_param_setting(sc, ft_vector_assembler, test_args)
})
