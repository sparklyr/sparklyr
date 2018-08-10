context("ml feature min max scaler")

test_that("ft_min_max_scaler() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_min_max_scaler)
})

test_that("ft_min_max_scaler() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    min = -1, max = 2
  )
  test_param_setting(sc, ft_min_max_scaler, test_args)
})

test_that("ft_min_max_scaler() works properly", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    id = 0:2,
    V1 = c(1, 2, 3),
    V2 = c(0.1, 1.1, 10.1),
    V3 = c(-1, 1, 3)
  )

  scaled_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:3), "features") %>%
    ft_min_max_scaler("features", "scaledFeatures") %>%
    pull(scaledFeatures)

  expect_equal(
    scaled_features,
    list(c(0, 0, 0),
         c(0.5, 0.1, 0.5),
         c(1, 1, 1))
  )
})

