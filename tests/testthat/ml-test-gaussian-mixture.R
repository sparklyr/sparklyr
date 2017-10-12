context("ml gaussian mixture")

sc <- testthat_spark_connection()
test_requires("dplyr")

test_that("ml_gaussian_mixture param setting", {
  args <- list(
    x = sc, k = 9, max_iter = 120, tol = 0.02,
    seed = 98, features_col = "fcol",
    prediction_col = "pcol", probability_col = "prcol"
  )
  predictor <- do.call(ml_gaussian_mixture, args)
  args_to_check <- setdiff(names(args), "x")

  expect_equal(ml_params(predictor, args_to_check), args[args_to_check])
})

test_that("ml_gaussian_mixture() default params are correct", {

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
