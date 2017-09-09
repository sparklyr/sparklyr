context("compile")

scalac_is_available <- function(version) {
  tryCatch({
    find_scalac(version)
    TRUE
  }, error = function(e) FALSE)
}

ensure_download_scalac <- function() {
  if (!scalac_is_available("2.10") || !scalac_is_available("2.11")) {
    download_scalac()
  }
}

test_that("'find_scalac' can find scala version", {
  ensure_download_scalac()

  expect_true(scalac_is_available("2.10"))
  expect_true(scalac_is_available("2.11"))
})

test_that("'spark_default_compilation_spec' can create default specification", {
  expect_gte(spark_default_compilation_spec(), 5)
})
