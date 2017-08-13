context("livy")
test_requires("dplyr")

test_that("'copy_to()' works under Livy connections", {
  lc <- testthat_livy_connection()

  df <- data.frame(a = c(1, 2), b = c("A", "B"))
  df_tbl <- copy_to(lc, df)

  expect_equal(df_tbl %>% collect(), df)
})
