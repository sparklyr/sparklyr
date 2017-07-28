context("spark apply bundle")

sc <- testthat_spark_connection()

test_that("'core_spark_apply_bundle' can `core_spark_apply_unbundle`", {
  bundlePath <- core_spark_apply_bundle()
  unbundlePath <- worker_spark_apply_unbundle(bundlePath, tempdir())

  unlink(bundlePath, recursive = TRUE)
  unlink(unbundlePath, recursive = TRUE)

  succeed()
})
