library(testthat)

context("Dplyr SQL")

test_that("window function is not found on expression", {
  expect_false(spark_dplyr_expression_contains(quote(1 + 2), "min_rank"))
})

test_that("window function is found on expression", {
  expect_true(spark_dplyr_expression_contains(quote(1 + min_rank(2)), "min_rank"))
})
