context("jfloat")

test_that("jfloat() works as expected", {
  sc <- testthat_spark_connection()
  x <- 1.23e-3
  jfl <- jfloat(sc, x)

  expect_true(inherits(jfl, "spark_jobj"))
  expect_equal(jfl %>% invoke("doubleValue"), x)
})
