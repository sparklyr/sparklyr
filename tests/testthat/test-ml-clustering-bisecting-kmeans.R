context("ml clustering - bisecting kmeans")

sc <- testthat_spark_connection()
test_requires("dplyr")
data(iris)

test_that("ml_bisecting_kmeans param setting", {
  args <- list(
    x = sc, k = 9, max_iter = 11, min_divisible_cluster_size = 3,
    seed = 98, features_col = "fcol",
    prediction_col = "pcol"
  )
  predictor <- do.call(ml_bisecting_kmeans, args)
  args_to_check <- setdiff(names(args), "x")

  expect_equal(ml_params(predictor, args_to_check), args[args_to_check])
})

test_that("ml_bisecting_kmeans() default params are correct", {

  predictor <- ml_pipeline(sc) %>%
    ml_bisecting_kmeans() %>%
    ml_stage(1)

  args <- get_default_args(
    ml_bisecting_kmeans,
    c("x", "uid", "...", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})
