context("generalized linear regression")

test_that("'ml_generalized_linear_regression' and 'glm' produce similar fits", {
  skip_on_cran()
  
  sc <- tryCatch(
    spark_connect("local", version = "2.0.0-preview"),
    error = function(e) skip("requires Spark 2.0.0")
  )
  
  on.exit(spark_disconnect(sc))
  
  mtcars_tbl <- copy_to(sc, mtcars, "mtcars")
  
  r <- glm(mpg ~ cyl + wt, data = mtcars, family = gaussian(link = "identity"))
  s <- ml_generalized_linear_regression(mtcars_tbl, "mpg", c("cyl", "wt"), family = gaussian(link = "identity"))
  expect_equal(coef(r), coef(s))
  
  beaver <- beaver2
  beaver_tbl <- copy_to(sc, beaver, overwrite = TRUE)
  
  r <- glm(data = beaver, activ ~ temp, family = binomial(link = "logit"))
  s <- ml_generalized_linear_regression(beaver_tbl, "activ", "temp", family = binomial(link = "logit"))
  expect_equal(coef(r), coef(s))
  
})
