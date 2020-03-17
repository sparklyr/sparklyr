context("describe")

test_requires("dplyr")

sc <- testthat_spark_connection()

test_that("sdf_describe() works properly", {
  iris_tbl <- testthat_tbl("iris")
  s <- sdf_describe(iris_tbl)
  expect_equal(colnames(s), c("summary", colnames(iris_tbl)))
  expect_equal(pull(s, summary), c("count", "mean", "stddev", "min", "max"))

  s <- sdf_describe(iris_tbl, "Sepal_Length")
  expect_equal(colnames(s), c("summary", "Sepal_Length"))
})

test_that("sdf_describe() checks column name", {
  iris_tbl <- testthat_tbl("iris")
  expect_error(sdf_describe(iris_tbl, c("Sepal_Length", "foo")),
               "The following columns are not in the data frame: foo")
})
