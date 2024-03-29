skip_connection("ml-feature-max-abs-scaler")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_max_abs_scaler() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar"
  )
  test_param_setting(sc, ft_max_abs_scaler, test_args)
})

test_that("ft_max_abs_scaler() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "ft_max_abs_scaler requires Spark 2.0.0+")
  df <- data.frame(
    id = 0:2,
    V1 = c(1, 2, 4),
    V2 = c(0.1, 1, 10),
    V3 = c(-8, -4, 8)
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    f_a <- df_tbl %>%
      ft_vector_assembler(paste0("V", 1:3), "features") %>%
      ft_max_abs_scaler("features", "scaled") %>%
      pull(scaled)
  )

  expect_equal(
    f_a,
    list(
      c(0.25, 0.01, -1),
      c(0.5, 0.1, -0.5),
      c(1, 1, 1)
    )
  )
})

test_clear_cache()

