context("livy")
test_requires("dplyr")

test_that("'spark_version()' works under Livy connections", {
  if (.Platform$OS.type == "windows")
    skip("Livy service unsupported in Windows")

  lc <- testthat_livy_connection()

  version <- spark_version(lc)
  version_expected <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  expect_equal(version, numeric_version(version_expected))
})

test_that("'copy_to()' works under Livy connections", {
  if (.Platform$OS.type == "windows")
    skip("Livy service unsupported in Windows")

  lc <- testthat_livy_connection()

  df <- data.frame(a = c(1, 2), b = c("A", "B"), stringsAsFactors = FALSE)
  df_tbl <- copy_to(lc, df)

  expect_equal(df_tbl %>% collect(), df)
})

test_that("'livy_config()' works with extended parameters", {
  if (.Platform$OS.type == "windows")
    skip("Livy service unsupported in Windows")

  config <- livy_config(num_executors = 1)

  expect_equal(as.integer(config$livy.numExecutors), 1)
})

test_that("'livy_config()' works with authentication", {
  if (.Platform$OS.type == "windows")
    skip("Livy service unsupported in Windows")

  config_basic <- livy_config(username = "foo", password = "bar")

  expect_equal(
    config_basic$sparklyr.livy.auth,
    httr::authenticate("foo", "bar", type = "basic")
  )

  config_negotiate <- livy_config(negotiate = TRUE)

  expect_equal(
    config_negotiate$sparklyr.livy.auth,
    httr::authenticate("", "", type = "gssnegotiate")
  )
})

test_that("'spark_apply()' works under Livy connections", {
  if (.Platform$OS.type == "windows")
    skip("Livy service unsupported in Windows")

  lc <- testthat_livy_connection()

  df <- data.frame(id = 10)
  apply_tbl <- sdf_len(lc, 1) %>% spark_apply(function(x) 10, packages = FALSE)

  expect_equal(apply_tbl %>% collect(), df)
})
