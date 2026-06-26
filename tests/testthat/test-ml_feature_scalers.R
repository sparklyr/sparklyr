skip_connection("ml_feature_scalers")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_max_abs_scaler() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar"
  )
  test_param_setting(sc, ft_max_abs_scaler, test_args)
})

test_that("ft_max_abs_scaler() works properly", {
  skip_databricks_connect()
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

test_that("ft_min_max_scaler() default params", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_min_max_scaler)
})

test_that("ft_min_max_scaler() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    min = -1,
    max = 2
  )
  test_param_setting(sc, ft_min_max_scaler, test_args)
})

test_that("ft_min_max_scaler() works properly", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  df <- data.frame(
    id = 0:2,
    V1 = c(1, 2, 3),
    V2 = c(0.1, 1.1, 10.1),
    V3 = c(-1, 1, 3)
  )

  expect_warning_on_arrow(
    scaled_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
      ft_vector_assembler(paste0("V", 1:3), "features") %>%
      ft_min_max_scaler("features", "scaledFeatures") %>%
      pull(scaledFeatures)
  )

  expect_equal(
    scaled_features,
    list(
      c(0, 0, 0),
      c(0.5, 0.1, 0.5),
      c(1, 1, 1)
    )
  )
})

test_that("ft_robust_scaler() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("3.0.0", "ft_robust_scaler requires Spark 3.0.0+")
  df <- data.frame(
    id = 1:5,
    V1 = c(0.0, 1.0, 2.0, 3.0, 4.0),
    V2 = c(0.0, -1.0, -2.0, -3.0, -4.0)
  )

  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    r_s <- df_tbl %>%
      ft_vector_assembler(paste0("V", 1:2), "features") %>%
      ft_robust_scaler("features", "scaled") %>%
      pull(scaled)
  )

  expect_equal(
    r_s,
    list(
      c(-1, 1),
      c(-0.5, 0.5),
      c(0, 0),
      c(0.5, -0.5),
      c(1, -1)
    )
  )
})

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
  sample_data <- spark_read_libsvm(
    sc,
    "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  scaler <- ft_standard_scaler(
    sc,
    input_col = "features",
    output_col = "scaledFeatures",
    with_std = TRUE,
    with_mean = FALSE,
    uid = "standard_scalaer_999"
  )

  scaler_model <- ml_fit(scaler, sample_data)

  expect_warning_on_arrow(
    s_m <- scaler_model %>%
      ml_transform(sample_data) %>%
      head(1) %>%
      dplyr::pull(scaledFeatures) %>%
      unlist() %>%
      sum()
  )

  expect_equal(
    s_m,
    295.3425,
    tolerance = 0.001,
    scale = 1
  )

  expect_output_file(
    print(scaler_model),
    output_file("print/standard-scaler-model.txt")
  )
})

test_clear_cache()
