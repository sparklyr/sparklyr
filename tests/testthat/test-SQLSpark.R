library(testthat)

context("Dplyr SQL")

test_that("window function is not found on expression", {
  expect_false(spark_dplyr_any_expression(quote(1 + 2), function(e) {
    as.character(e) == "min_rank"
  }))
})

test_that("window function is found on expression", {
  expect_true(spark_dplyr_any_expression(quote(1 + min_rank(2)), function(e) {
    as.character(e) == "min_rank"
  }))
})
