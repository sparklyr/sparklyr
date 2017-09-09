context("compile")

scalac_is_available <- function(version, download_path) {
  tryCatch({
    find_scalac(version, download_path)
    TRUE
  }, error = function(e) FALSE)
}

ensure_download_scalac <- function(download_path) {
  if (!scalac_is_available("2.10", download_path) || !scalac_is_available("2.11", download_path)) {
    download_scalac(download_path)
  }
}

scalac_download_path <- tempdir()

test_that("'find_scalac' can find scala version", {
  ensure_download_scalac(scalac_download_path)

  expect_true(scalac_is_available("2.10", scalac_download_path))
  expect_true(scalac_is_available("2.11", scalac_download_path))
})

test_that("'spark_default_compilation_spec' can create default specification", {
  spec <- spark_default_compilation_spec(locations = scalac_download_path)
  expect_gte(length(spec), 5)
})
