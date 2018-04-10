context("ml regression - linear regression")
sc <- testthat_spark_connection()

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  # print(data.frame(lhs = lhs, rhs = rhs))

  expect_true(all.equal(lhs, rhs, tolerance = 0.01))
}

test_that("ml_linear_regression param setting works", {
  lr <- ml_linear_regression(
    sc,
    features_col = "fcol",
    label_col = "lcol",
    fit_intercept = FALSE,
    elastic_net_param = 0.1,
    reg_param = 0.2,
    max_iter = 50L,
    weight_col = "wcol",
    prediction_col = "pcol",
    solver = "l-bfgs",
    standardization = FALSE,
    tol = 1e-5)
  expect_equal(
    ml_params(lr, list(
      "features_col", "label_col", "fit_intercept", "elastic_net_param",
      "reg_param", "max_iter", "weight_col", "prediction_col", "solver",
      "standardization", "tol"
    )),
    list(
      features_col = "fcol",
      label_col = "lcol",
      fit_intercept = FALSE,
      elastic_net_param = 0.1,
      reg_param = 0.2,
      max_iter = 50L,
      weight_col = "wcol",
      prediction_col = "pcol",
      solver = "l-bfgs",
      standardization = FALSE,
      tol = 1e-5
    )
  )

  lr2 <- ml_linear_regression(sc, alpha = 0.2, lambda = 0.1)
  expect_equal(ml_params(lr2, c("elastic_net_param", "reg_param")),
               list(elastic_net_param = 0.2, reg_param = 0.1))
})

test_that("ml_linear_regression() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_linear_regression() %>%
    ml_stage(1)

  args <- get_default_args(ml_linear_regression,
                           exclude = c("x", "uid", "...", "weight_col", "loss"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_linear_regression and 'penalized' produce similar model fits", {
  skip_on_cran()
  test_requires("glmnet")

  mtcars_tbl <- testthat_tbl("mtcars")

  values <- seq(0, 0.5, by = 0.1)
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
      alpha = alpha,
      lambda = lambda
    )

    gCoef <- coefficients(gFit)[, 1]
    sCoef <- coefficients(sFit)

    expect_coef_equal(gCoef, sCoef)
  }

})

test_that("weights column works for lm", {
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
                            lambda = 0L,
                            weights.column = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)))

  r <- lm(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
          data = iris_weighted)
  s <- ml_linear_regression(iris_weighted_tbl,
                            response = "Sepal_Length",
                            features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                            lambda = 0L,
                            weights.column = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)))
})

test_that("ml_linear_regression print methods work", {
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
  iris_tbl <- testthat_tbl("iris")
  m <- ml_linear_regression(iris_tbl, Petal_Width ~ Petal_Length)
  expect_equal(
    length(fitted(m)),
    nrow(iris)
  )
})
