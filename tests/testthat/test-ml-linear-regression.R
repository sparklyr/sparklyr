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
