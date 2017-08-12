context("livy")
test_requires("dplyr")

lc <- testthat_livy_connection()

test_that("'copy_to()' works under Livy connections", {
  df <- data.frame(a = c(1, 2), b = c("A", "B"))
  df_tbl <- copy_to(lc, df)

  expect_equal(df_tbl %>% collect(), df)
})
