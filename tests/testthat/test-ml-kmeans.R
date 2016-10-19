context("clustering")
sc <- testthat_spark_connection()

expect_centers_equal <- function(lhs, rhs, ...) {
  nm <- colnames(lhs)
  lhs <- lhs[, nm]
  rhs <- rhs[, nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01, ...))
}


test_that("'ml_kmeans' and 'kmeans' produce similar fits", {
  skip_on_cran()
  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  library(dplyr)
  data(iris)

  iris_tbl <- testthat_tbl("iris")

  set.seed(123)
  iris <- iris %>%
    rename(Sepal_Length = Sepal.Length,
           Petal_Length = Petal.Length)

  r <-        kmeans(iris %>% dplyr::select(Sepal_Length, Petal_Length), centers = 3, iter.max = 5)
  s <- ml_kmeans(iris_tbl %>% dplyr::select(Sepal_Length, Petal_Length), centers = 3, max.iter = 5)
  expect_centers_equal(r$centers, as.matrix(s$centers), # NOTE THAT s$centers is a data.frame
                       check.attributes = FALSE)

})
