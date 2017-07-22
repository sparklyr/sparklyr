context("spark apply bundle")

sc <- testthat_spark_connection()

test_that("'spark_apply_bundle' can `spark_apply_unbundle`", {
  bundlePath <- spark_apply_bundle()
  unbundlePath <- spark_apply_unbundle(bundlePath)

  unlink(bundlePath, recursive = TRUE)
  unlink(unbundlePath, recursive = TRUE)

  succeed()
})
