context("spark apply bundle")

sc <- testthat_spark_connection()

test_that("'spark_apply_bundle' can `worker_spark_apply_unbundle`", {
  bundlePath <- spark_apply_bundle()
  unbundlePath <- worker_spark_apply_unbundle(bundlePath, tempdir(), "package")

  unlink(bundlePath, recursive = TRUE)
  unlink(unbundlePath, recursive = TRUE)

  succeed()
})

available_packages_mock <- function() {
  packages_sample = dir(
    getwd(),
    recursive = TRUE,
    pattern = "packages-sample.rds",
    full.names = TRUE)

  as.matrix(
    readRDS(file = packages_sample)
  )
}

test_that("'spark_apply_packages' uses different names for different packages", {
  with_mock(
    `available.packages` = available_packages_mock,
    expect_true(
      length(spark_apply_packages("purrr")) > 0
    )
  )
})

test_that("'spark_apply_bundle_file' uses different names for different packages", {
  purrr_file <- spark_apply_bundle_file(spark_apply_packages("purrr"), tempdir())
  tidyr_file <- spark_apply_bundle_file(spark_apply_packages("tidyr"), tempdir())

  expect_true(purrr_file != tidyr_file)
})
