context("ml regression - linear regression")

test_that("ml_linear_regression() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_linear_regression)
})

test_that("ml_linear_regression() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    fit_intercept = FALSE,
    elastic_net_param = 0.1,
    reg_param = 0.1,
    max_iter = 40,
    weight_col = "wcol",
    loss = "huber",
    solver = "normal",
    standardization = FALSE,
    tol = 1e-04,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol"
  )
  test_param_setting(sc, ml_linear_regression, test_args)
})

test_that("ml_linear_regression and 'penalized' produce similar model fits", {
  test_requires("glmnet")
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")

  values <- seq(0, 0.4, by = 0.4)
  parMatrix <- expand.grid(values, values, KEEP.OUT.ATTRS = FALSE)

  for (i in seq_len(nrow(parMatrix))) {
    alpha  <- parMatrix[[1]][[i]]
    lambda <- parMatrix[[2]][[i]]

    gFit <- glmnet::glmnet(
      x = as.matrix(mtcars[, c("cyl", "disp")]),
      y = mtcars$mpg,
      family = "gaussian",
      alpha = alpha,
      lambda = lambda
    )

    sFit <- ml_linear_regression(
      mtcars_tbl,
      response = "mpg",
      features = c("cyl", "disp"),
      elastic_net_param = alpha,
      reg_param = lambda
    )

    gCoef <- coefficients(gFit)[, 1]
    sCoef <- coefficients(sFit)

    expect_coef_equal(gCoef, sCoef)
  }

})

test_that("weights column works for lm", {
  sc <- testthat_spark_connection()
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  r <- lm(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
          weights = weights, data = iris_weighted)
  s <- ml_linear_regression(iris_weighted_tbl,
                            response = "Sepal_Length",
                            features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                            reg_param = 0L,
                            weight_col = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)))

  r <- lm(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
          data = iris_weighted)
  s <- ml_linear_regression(iris_weighted_tbl,
                            response = "Sepal_Length",
                            features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                            reg_param = 0L,
                            weight_col = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)))
})

test_that("ml_linear_regression print methods work", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  linear_model <- ml_linear_regression(
    iris_tbl, Petal_Length ~ Petal_Width)

  expect_known_output(
    linear_model,
    output_file("print/linear-regression.txt"),
    print = TRUE
  )

  expect_known_output(
    summary(linear_model),
    output_file("print/linear-regression-summary.txt"),
    print = TRUE
  )
})

test_that("fitted() works for linear regression", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  m <- ml_linear_regression(iris_tbl, Petal_Width ~ Petal_Length)
  expect_equal(
    length(fitted(m)),
    nrow(iris)
  )
})
