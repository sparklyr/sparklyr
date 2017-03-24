context("clustering")

sc <- testthat_spark_connection()
test_requires("dplyr")
data(iris)

test_that("'ml_kmeans' and 'kmeans' produce similar fits", {
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
    ml_kmeans(centers = 3L)

  lhs <- as.matrix(R$centers)
  rhs <- as.matrix(S$centers)

  # ensure lhs, rhs are in same order (since labels may
  # not match between the two fits)
  lhs <- lhs[order(lhs[, 1]), ]
  rhs <- rhs[order(rhs[, 1]), ]
  expect_equivalent(lhs, rhs)

})
