context("livy")
test_requires("dplyr")

test_that("'copy_to()' works under Livy connections", {
  lc <- testthat_livy_connection()

  version <- spark_version(lc)
  expect_equal(version, "2.1.0")
})

test_that("'copy_to()' works under Livy connections", {
  lc <- testthat_livy_connection()

  df <- data.frame(a = c(1, 2), b = c("A", "B"), stringsAsFactors = FALSE)
  df_tbl <- copy_to(lc, df)

  expect_equal(df_tbl %>% collect(), df)
})

test_that("'livy_config()' works with extended parameters", {
  config <- livy_config(num_executors = 1)

  expect_equal(config$livy.numExecutors, 1)
})
