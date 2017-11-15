context("ml regression - glm")

sc <- testthat_spark_connection()

test_that("ml_generalized_linear_regression() sets params correctly", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
  glr <- ml_generalized_linear_regression(
    sc,
    family = "binomial",
    link = "logit",
    features_col = "fcol",
    label_col = "lcol",
    fit_intercept = FALSE,
    link_prediction_col = "lpcol",
    reg_param = 0.1,
    max_iter = 50L,
    weight_col = "wcol",
    prediction_col = "pcol",
    tol = 1e-5
  )
  expect_equal(
    ml_params(glr, list(
      "family", "link", "features_col", "label_col", "fit_intercept",
      "link_prediction_col", "reg_param", "max_iter", "weight_col",
      "prediction_col", "tol"
    )),
    list(
      family = "binomial",
      link = "logit",
      features_col = "fcol",
      label_col = "lcol",
      fit_intercept = FALSE,
      link_prediction_col = "lpcol",
      reg_param = 0.1,
      max_iter = 50L,
      weight_col = "wcol",
      prediction_col = "pcol",
      tol = 1e-5
    )
  )
})

test_that("ml_generalized_linear_regression() default params are correct", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
  predictor <- ml_pipeline(sc) %>%
    ml_generalized_linear_regression() %>%
    ml_stage(1)

  args <- get_default_args(ml_generalized_linear_regression,
                           c("x", "uid", "...", "weight_col", "link", "link_power", "link_prediction_col",
                             "variance_power"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("'ml_generalized_linear_regression' and 'glm' produce similar fits and residuals", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
  skip_on_cran()

  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  mtcars_tbl <- testthat_tbl("mtcars")

  r <- glm(mpg ~ cyl + wt, data = mtcars, family = gaussian(link = "identity"))
  s <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg",
                                        features = c("cyl", "wt"), family = gaussian(link = "identity"))
  expect_equal(coef(r), coef(s))
  expect_equal(residuals(r) %>% unname(), residuals(s))
  df_r <- mtcars %>%
    mutate(residuals = unname(residuals(r)))
  df_s <- sdf_residuals(s) %>%
    collect() %>%
    as.data.frame()
  expect_equal(df_r, df_s)

  beaver_tbl <- testthat_tbl("beaver2")

  r <- glm(data = beaver2, activ ~ temp, family = binomial(link = "logit"))
  s <- ml_generalized_linear_regression(beaver_tbl, response = "activ",
                                        features = "temp", family = binomial(link = "logit"))
  expect_equal(coef(r), coef(s))
  expect_equal(residuals(r) %>% unname(), residuals(s))
  df_r <- beaver2 %>%
    mutate(residuals = unname(residuals(r)))
  df_s <- sdf_residuals(s) %>%
    as.data.frame()
  expect_equal(df_r, df_s)

})

test_that("weights column works for glm", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
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

test_that("ml_generalized_linear_regression print methods work", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
  iris_tbl <- testthat_tbl("iris")
  print_output <- ml_generalized_linear_regression(
    iris_tbl, Petal_Length ~ Petal_Width) %>%
    capture.output()
  expect_equal(print_output[4], "(Intercept) Petal_Width ")
  expect_match(print_output[7], "Degress of Freedom")

  summary_output <- capture.output(summary(ml_generalized_linear_regression(
    iris_tbl, Petal_Length ~ Petal_Width)))

  expect_equal(summary_output[8], "(Intercept) Petal_Width ")
  expect_match(summary_output[13], "Null")
})
