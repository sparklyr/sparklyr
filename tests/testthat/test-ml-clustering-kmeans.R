skip_connection("ml-clustering-kmeans")
skip_on_livy()
skip_on_arrow_devel()

test_that("ml_kmeans() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_kmeans)
})

test_clear_cache()

