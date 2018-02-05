context("ml clustering - kmeans")

sc <- testthat_spark_connection()
test_requires("dplyr")
data(iris)

test_that("ml_kmeans param setting", {
  args <- list(
    x = sc, k = 9, max_iter = 11, tol = 1e-5,
    init_steps = 3L, init_mode = "random",
    seed = 98, features_col = "fcol",
    prediction_col = "pcol"
  )
  predictor <- do.call(ml_kmeans, args)
  args_to_check <- setdiff(names(args), "x")

  expect_equal(ml_params(predictor, args_to_check), args[args_to_check])
})

test_that("ml_kmeans() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_kmeans() %>%
    ml_stage(1)

  args <- get_default_args(
    ml_kmeans,
    c("x", "uid", "...", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("'ml_kmeans' and 'kmeans' produce similar fits", {
  test_requires_version("2.0.0", "ml_kmeans() requires Spark 2.0.0+")
  skip_on_cran()

  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  iris_tbl <- testthat_tbl("iris")

  set.seed(123)
  iris <- iris %>%
    rename(Sepal_Length = Sepal.Length,
           Petal_Length = Petal.Length)

  R <- iris %>%
    select(Sepal_Length, Petal_Length) %>%
    kmeans(centers = 3)

  S <- iris_tbl %>%
    select(Sepal_Length, Petal_Length) %>%
    ml_kmeans(~ ., centers = 3L)

  lhs <- as.matrix(R$centers)
  rhs <- as.matrix(S$centers)

  # ensure lhs, rhs are in same order (since labels may
  # not match between the two fits)
  lhs <- lhs[order(lhs[, 1]), ]
  rhs <- rhs[order(rhs[, 1]), ]
  expect_equivalent(lhs, rhs)

})

test_that("'ml_kmeans' supports 'features' argument for backwards compat (#1150)", {

  iris_tbl <- testthat_tbl("iris")

  set.seed(123)
  iris <- iris %>%
    rename(Sepal_Length = Sepal.Length,
           Petal_Length = Petal.Length)

  R <- iris %>%
    select(Sepal_Length, Petal_Length) %>%
    kmeans(centers = 3)

  S <- iris_tbl %>%
    select(Sepal_Length, Petal_Length) %>%
    ml_kmeans(centers = 3L, features = c("Sepal_Length", "Petal_Length"))

  lhs <- as.matrix(R$centers)
  rhs <- as.matrix(S$centers)

  # ensure lhs, rhs are in same order (since labels may
  # not match between the two fits)
  lhs <- lhs[order(lhs[, 1]), ]
  rhs <- rhs[order(rhs[, 1]), ]
  expect_equivalent(lhs, rhs)

})

test_that("ml_kmeans() works properly", {
  iris_tbl <- testthat_tbl("iris")
  iris_kmeans <- ml_kmeans(iris_tbl, ~ . - Species, centers = 5, seed = 11)
  expect_equal(ml_predict(iris_kmeans, iris_tbl) %>%
    dplyr::distinct(prediction) %>%
    dplyr::arrange(prediction) %>%
    dplyr::pull(prediction),
    0:4)
})

test_that("ml_compute_cost() for kmeans works properly", {
  test_requires_version("2.0.0", "ml_compute_cost() requires Spark 2.0+")
  iris_tbl <- testthat_tbl("iris")
  iris_kmeans <- ml_kmeans(iris_tbl, ~ . - Species, centers = 5, seed = 11)
  expect_equal(
    ml_compute_cost(iris_kmeans, iris_tbl),
    46.7123, tolerance = 0.01
  )
  expect_equal(
    iris_tbl %>%
      ft_r_formula(~ . - Species) %>%
      ml_compute_cost(iris_kmeans$model, .),
    46.7123, tolerance = 0.01
  )
})
