context("glm")

sc <- testthat_spark_connection()

test_that("'ml_generalized_linear_regression' and 'glm' produce similar fits and residuals", {
  skip_on_cran()

  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  mtcars_tbl <- testthat_tbl("mtcars")

  r <- glm(mpg ~ cyl + wt, data = mtcars, family = gaussian(link = "identity"))
  s <- ml_generalized_linear_regression(mtcars_tbl, "mpg", c("cyl", "wt"), family = gaussian(link = "identity"))
  expect_equal(coef(r), coef(s))
  expect_equal(residuals(r) %>% unname(), residuals(s))
  df_r <- mtcars %>%
    mutate(residuals = unname(residuals(r)))
  df_s <- sdf_residuals(s) %>%
    collect() %>%
    as.data.frame()
  expect_equal(df_r, df_s)

  beaver <- beaver2
  beaver_tbl <- testthat_tbl("beaver2")

  r <- glm(data = beaver, activ ~ temp, family = binomial(link = "logit"))
  s <- ml_generalized_linear_regression(beaver_tbl, "activ", "temp", family = binomial(link = "logit"))
  expect_equal(coef(r), coef(s))
  expect_equal(residuals(r) %>% unname(), residuals(s))
  df_r <- beaver %>%
    mutate(residuals = unname(residuals(r)))
  df_s <- sdf_residuals(s) %>%
    as.data.frame()
  expect_equal(df_r, df_s)

})

test_that("weights column works for glm", {
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  r <- glm(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
           family = gaussian, weights = weights, data = iris_weighted)
  s <- ml_generalized_linear_regression(iris_weighted_tbl,
                                        response = "Sepal_Length",
                                        features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                                        weights.column = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)))

  r <- glm(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
           family = gaussian, data = iris_weighted)
  s <- ml_generalized_linear_regression(iris_weighted_tbl,
                                        response = "Sepal_Length",
                                        features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                                        weights.column = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)))
})
