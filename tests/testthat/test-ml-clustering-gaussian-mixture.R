context("ml gaussian mixture")

sc <- testthat_spark_connection()
test_requires("dplyr")

test_that("ml_gaussian_mixture param setting", {
  test_requires_version("2.0.0", "gaussian mixture requires 2.0+")
  args <- list(
    x = sc, k = 9, max_iter = 120, tol = 0.02,
    seed = 98, features_col = "fcol",
    prediction_col = "pcol", probability_col = "prcol"
  )
  predictor <- do.call(ml_gaussian_mixture, args)

  expect_equal(ml_params(predictor, names(args)[-1]), args[-1])
})

test_that("ml_gaussian_mixture() default params are correct", {
  test_requires_version("2.0.0", "gaussian mixture requires 2.0+")
  predictor <- ml_pipeline(sc) %>%
    ml_gaussian_mixture() %>%
    ml_stage(1)

  args <- get_default_args(
    ml_gaussian_mixture,
    c("x", "uid", "...", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_gaussian_mixture() works properly", {
  test_requires_version("2.0.0", "gaussian mixture requires 2.0+")
  sample_data_path <- dir(getwd(), recursive = TRUE, pattern = "sample_kmeans_data.txt", full.names = TRUE)
  sample_data <- spark_read_libsvm(sc, "sample_data",
                                   sample_data_path, overwrite = TRUE)

  gmm <- ml_gaussian_mixture(sample_data, k = 2, seed = 1)

  expect_equal(gmm$weights, c(0.5, 0.5))
  expect_equal(gmm$gaussians_df %>% pull(mean),
               list(c(0.1, 0.1, 0.1), c(9.1, 9.1, 9.1)))
  expect_equal(gmm$gaussians_df %>% pull(cov),
               list(matrix(rep(0.00666666667, 9), nrow = 3),
                    matrix(rep(0.00666666667, 9), nrow = 3))
  )
})
