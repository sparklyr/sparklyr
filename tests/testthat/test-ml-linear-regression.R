context("lm")
sc <- testthat_spark_connection()

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01))
}

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
      "mpg",
      c("cyl", "disp"),
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
