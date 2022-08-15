skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_vector_assembler() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "baz"),
    output_col = "bar"
  )
  test_param_setting(sc, ft_vector_assembler, test_args)
})
