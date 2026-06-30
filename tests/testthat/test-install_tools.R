skip_connection("install_tools")
skip_on_livy()
skip_on_arrow_devel()

test_that("spark_install_sync copies the sparkinstall sources into place", {
  # Build a fake spark-install project with the expected source files, and a
  # separate destination working dir, so we never touch the real package files.
  proj <- withr::local_tempdir()
  sources <- c(
    "R/install.R",
    "R/versions.R",
    "R/windows.R",
    "inst/extdata/config.json",
    "inst/extdata/versions.json"
  )
  dir.create(file.path(proj, "R"), recursive = TRUE)
  dir.create(file.path(proj, "inst", "extdata"), recursive = TRUE)
  for (f in sources) {
    writeLines("placeholder", file.path(proj, f))
  }

  dest <- withr::local_tempdir()
  withr::local_dir(dest)
  dir.create("R")
  dir.create(file.path("inst", "extdata"), recursive = TRUE)

  spark_install_sync(proj)

  expect_true(file.exists(file.path(dest, "R", "install_spark.R")))
  expect_true(file.exists(file.path(dest, "R", "install_spark_versions.R")))
  expect_true(file.exists(file.path(dest, "R", "install_spark_windows.R")))
  expect_true(file.exists(file.path(dest, "inst", "extdata", "versions.json")))
})

test_clear_cache()
