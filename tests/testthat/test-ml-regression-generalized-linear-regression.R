context("ml regression - glm")

test_that("ml_generalized_linear_regression() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_generalized_linear_regression)
})

test_that("ml_generalized_linear_regression() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    family = "poisson",
    link = "sqrt",
    fit_intercept = FALSE,
    offset_col = "ocol",
    link_power = 0.5,
    link_prediction_col = "lpcol",
    reg_param = 0.01,
    max_iter = 40,
    weight_col = "wcol",
    solver = "irls",
    tol = 1e-03,
    variance_power = 200,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol"
  )
  test_param_setting(sc, ml_generalized_linear_regression, test_args)
})

test_that("'ml_generalized_linear_regression' and 'glm' produce similar fits and residuals", {
  test_requires_version("2.0.0", "glm requires spark 2.0+")
  sc <- testthat_spark_connection()
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
  sc <- testthat_spark_connection()
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
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  glm_model <- ml_generalized_linear_regression(
    iris_tbl, Petal_Length ~ Petal_Width)

  expect_known_output(
    glm_model,
    output_file("print/glm.txt"),
    print = TRUE
  )

  expect_known_output(
    summary(glm_model),
    output_file("print/glm-summary.txt"),
    print = TRUE
  )
})
