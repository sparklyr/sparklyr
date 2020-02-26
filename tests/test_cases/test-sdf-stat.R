context("sdf stat")

test_that("sdf_crosstab() works", {
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")
  df <- mtcars_tbl %>% sdf_crosstab("cyl", "gear") %>% collect()
  expect_setequal(names(df), c("cyl_gear", "3.0", "4.0", "5.0"))
  expect_setequal(df[,1, drop = TRUE], c("8.0", "4.0", "6.0"))
})
