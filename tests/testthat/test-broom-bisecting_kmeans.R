context("broom-bisecting_kmeans")

test_that("bisecting_kmeans.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  td1 <- mtcars_tbl %>%
    ml_bisecting_kmeans(~ mpg + cyl, k = 4L, seed = 123) %>%
    tidy()

  check_tidy(td1, exp.row = 4,
             exp.names = c("mpg", "cyl", "size", "cluster"))
  expect_equal(td1$size, c(11, 9, 7, 5))

})
