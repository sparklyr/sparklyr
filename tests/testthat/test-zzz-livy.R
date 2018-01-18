context("livy")
test_requires("dplyr")

test_that("'spark_version()' works under Livy connections", {
  lc <- testthat_livy_connection()

  version <- spark_version(lc)
  version_expected <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  expect_equal(version, numeric_version(version_expected))
})

test_that("'copy_to()' works under Livy connections", {
  lc <- testthat_livy_connection()

  df <- data.frame(a = c(1, 2), b = c("A", "B"), stringsAsFactors = FALSE)
  df_tbl <- copy_to(lc, df)

  expect_equal(df_tbl %>% collect(), df)
})

test_that("'livy_config()' works with extended parameters", {
  config <- livy_config(num_executors = 1)

  expect_equal(as.integer(config$livy.numExecutors), 1)
})

test_that("'livy_config()' works with authentication", {
  config <- livy_config(username = "foo", password = "bar")

  expect_equal(
    config$sparklyr.livy.headers$Authorization,
    "Basic Zm9vOmJhcg=="
  )
})
