skip_connection("ml-feature-hashing-tf")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_hashing_tf() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_hashing_tf)
})

test_that("ft_hashing_tf() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    binary = TRUE,
    num_features = 2^10
  )
  test_param_setting(sc, ft_hashing_tf, test_args)
})

test_clear_cache()

