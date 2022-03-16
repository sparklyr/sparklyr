skip_on_livy()
skip_on_arrow_devel()

scalac_is_available <- function(version, download_path) {
  tryCatch(
    {
      find_scalac(version, download_path)
      TRUE
    },
    error = function(e) FALSE
  )
}

ensure_download_scalac <- function(download_path) {
  for (scala_version in c("2.10", "2.11", "2.12")) {
    if (!scalac_is_available(scala_version, download_path)) {
      download_scalac(download_path)
      break
    }
  }
}

scalac_download_path <- tempdir()

test_that("'find_scalac' can find scala version", {
  ensure_download_scalac(scalac_download_path)

  expect_true(scalac_is_available("2.10", scalac_download_path))
  expect_true(scalac_is_available("2.11", scalac_download_path))
  expect_true(scalac_is_available("2.12", scalac_download_path))
})

test_that("'spark_default_compilation_spec' can create default specification", {
  ensure_download_scalac(scalac_download_path)

  spec <- spark_default_compilation_spec(locations = scalac_download_path)
  expect_gte(length(spec), 3)
})
