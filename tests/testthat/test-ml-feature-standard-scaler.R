skip_connection("ml-feature-standard-scaler")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_standard_scaler() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_standard_scaler)
})

test_that("ft_standard_scaler() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    with_mean = TRUE,
    with_std = TRUE
  )
  test_param_setting(sc, ft_standard_scaler, test_args)
})

test_that("ft_standard_scaler() works properly", {
  sc <- testthat_spark_connection()
  sample_data_path <- get_test_data_path("sample_libsvm_data.txt")
  sample_data <- spark_read_libsvm(sc, "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  scaler <- ft_standard_scaler(
    sc,
    input_col = "features", output_col = "scaledFeatures",
    with_std = TRUE, with_mean = FALSE, uid = "standard_scalaer_999"
  )

  scaler_model <- ml_fit(scaler, sample_data)

  expect_warning_on_arrow(
    s_m <-scaler_model %>%
      ml_transform(sample_data) %>%
      head(1) %>%
      dplyr::pull(scaledFeatures) %>%
      unlist() %>%
      sum()
  )

  expect_equal(
    s_m,
    295.3425,
    tolerance = 0.001, scale = 1
  )

  expect_output_file(
    print(scaler_model),
    output_file("print/standard-scaler-model.txt")
  )
})

test_clear_cache()

