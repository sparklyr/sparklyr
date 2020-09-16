context("sdf stat")

test_that("sdf_crosstab() works", {
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")
  df <- mtcars_tbl %>%
    sdf_crosstab("cyl", "gear") %>%
    collect()
  expect_setequal(names(df), c("cyl_gear", "3.0", "4.0", "5.0"))
  expect_setequal(df[, 1, drop = TRUE], c("8.0", "4.0", "6.0"))
})

test_that("sdf_quantile() works for a single column", {
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")
  quantiles <- mtcars_tbl %>%
    sdf_quantile(column = "disp")
  expect_mapequal(quantiles,
                  c(
                    `0%` = 71.1,
                    `25%` = 120.3,
                    `50%` = 167.6,
                    `75%` = 318,
                    `100%` = 472
                  ))

})

test_that("sdf_quantile() works for multiple column", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "multicolumn quantile requires 2.0+")
  mtcars_tbl <- testthat_tbl("mtcars")

  quantiles <- mtcars_tbl %>%
    sdf_quantile(column = c("disp", "drat"))
  expect_named(quantiles, c("disp", "drat"))
  expect_mapequal(quantiles[["disp"]],
                  c(
                    `0%` = 71.1,
                    `25%` = 120.3,
                    `50%` = 167.6,
                    `75%` = 318,
                    `100%` = 472
                  ))
  expect_mapequal(quantiles[["drat"]],
                  c(
                    `0%` = 2.76,
                    `25%` = 3.08,
                    `50%` = 3.69,
                    `75%` = 3.92,
                    `100%` = 4.93
                  ))
})
