context("glm")

sc <- testthat_spark_connection()

test_that("'ml_generalized_linear_regression' and 'glm' produce similar fits", {
  skip_on_cran()

  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  mtcars_tbl <- testthat_tbl("mtcars")

  r <- glm(mpg ~ cyl + wt, data = mtcars, family = gaussian(link = "identity"))
  s <- ml_generalized_linear_regression(mtcars_tbl, "mpg", c("cyl", "wt"), family = gaussian(link = "identity"))
  expect_equal(coef(r), coef(s))

  beaver <- beaver2
  beaver_tbl <- testthat_tbl("beaver2")

  r <- glm(data = beaver, activ ~ temp, family = binomial(link = "logit"))
  s <- ml_generalized_linear_regression(beaver_tbl, "activ", "temp", family = binomial(link = "logit"))
  expect_equal(coef(r), coef(s))

})
