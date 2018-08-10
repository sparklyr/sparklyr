context("ml clustering - kmeans")

test_that("ml_kmeans() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_kmeans)
})

test_that("ml_kmeans() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    k = 3,
    max_iter = 30,
    init_steps = 4,
    init_mode = "random",
    seed = 234,
    features_col = "wfaaefa",
    prediction_col = "awiefjaw"
  )
  test_param_setting(sc, ml_kmeans, test_args)
})

test_that("'ml_kmeans' and 'kmeans' produce similar fits", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "ml_kmeans() requires Spark 2.0.0+")

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
  sc <- testthat_spark_connection()
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  iris_kmeans <- ml_kmeans(iris_tbl, ~ . - Species, centers = 5, seed = 11)
  expect_equal(ml_predict(iris_kmeans, iris_tbl) %>%
    dplyr::distinct(prediction) %>%
    dplyr::arrange(prediction) %>%
    dplyr::pull(prediction),
    0:4)
})

test_that("ml_compute_cost() for kmeans works properly", {
  sc <- testthat_spark_connection()
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
