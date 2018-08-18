context("broom-gaussian_mixture")

test_that("gaussian_mixture.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  td1 <- mtcars_tbl %>%
    ml_gaussian_mixture(~ mpg + cyl, k = 4L, seed = 123) %>%
    tidy()

  check_tidy(td1, exp.row = 4,
             exp.names = c("mpg", "cyl", "weight", "size", "cluster"))
  expect_equal(td1$size, c(3, 14, 4, 11))

})
