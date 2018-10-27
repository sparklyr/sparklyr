context("ml clustering - kmeans")

test_that("ml_kmeans() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_kmeans)
})
