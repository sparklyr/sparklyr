skip_connection("ml-feature-polynomial-expansion")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_polynomial_expansion() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_polynomial_expansion)
})

test_that("ft_polynomial_expansion() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    degree = 4
  )
  test_param_setting(sc, ft_polynomial_expansion, test_args)
})

test_that("ft_polynomial_expansion() works properly", {
  sc <- testthat_spark_connection()
  df <- data.frame(V1 = 2, V2 = 1)

  expect_warning_on_arrow(
    poly_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
      ft_vector_assembler(c("V1", "V2"), "features") %>%
      ft_polynomial_expansion("features", "polyFeatures", degree = 3) %>%
      pull(polyFeatures)
  )

  expect_equal(
    poly_features,
    list(c(2, 4, 8, 1, 2, 4, 1, 2, 1))
  )
})

test_clear_cache()

