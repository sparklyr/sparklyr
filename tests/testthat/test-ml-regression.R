context("regression")

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01))
}

test_that("ml_lm and 'penalized' produce similar model fits", {
  skip_on_cran()
  skip_if_not_installed("glmnet")

  sc <- spark_connect("local", cores = "auto", version = "2.0.0-preview")
  db <- src_spark(sc)

  copy_to(db, mtcars, "mtcars")
  mtcars_tbl <- tbl(db, "mtcars")

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

    sFit <- ml_lm(
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
