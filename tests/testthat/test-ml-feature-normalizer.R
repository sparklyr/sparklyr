skip_connection("ml-feature-normalizer")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_normalizer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_normalizer)
})

test_that("ft_normalizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    p = 2
  )
  test_param_setting(sc, ft_normalizer, test_args)
})

test_that("ft_normalizer works properly", {
  sc <- testthat_spark_connection()
  df <- tribble(
    ~id, ~V1, ~V2, ~V3,
    0, 1, 0.5, -1,
    1, 2, 1, 1,
    2, 4, 10, 2
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:3), "features")

  expect_warning_on_arrow(
    norm_data1 <- df_tbl %>%
      ft_normalizer("features", "normFeatures", p = 1) %>%
      pull(normFeatures)
  )

  expect_equal(
    norm_data1,
    list(
      c(0.4, 0.2, -0.4),
      c(0.5, 0.25, 0.25),
      c(0.25, 0.625, 0.125)
    )
  )

  expect_warning_on_arrow(
    norm_data2 <- df_tbl %>%
      ft_normalizer("features", "normFeatures", p = Inf) %>%
      pull(normFeatures)
  )

  expect_equal(
    norm_data2,
    list(
      c(1, 0.5, -1),
      c(1, 0.5, 0.5),
      c(0.4, 1, 0.2)
    )
  )
})

test_that("ft_normalizer errors for bad p", {
  sc <- testthat_spark_connection()
  expect_error(
    ft_normalizer(sc, "features", "normFeatures", p = 0.5),
    "`p` must be at least 1\\."
  )
})

test_clear_cache()

