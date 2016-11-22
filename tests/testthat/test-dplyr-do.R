context("dplyr do")
sc <- testthat_spark_connection()

test_requires("ggplot2")
diamonds_tbl <- testthat_tbl("diamonds")

test_that("the (serial) implementation of 'do' functions as expected", {
  test_requires("dplyr")

  R <- diamonds %>%
    group_by(color, clarity) %>%
    do(model = lm(price ~ x + y + z, data = .))

  S <- diamonds_tbl %>%
    group_by(color, clarity) %>%
    do(model = ml_linear_regression(., price ~ x + y + z))

  R <- arrange(R, as.character(color), as.character(clarity))
  S <- arrange(S, as.character(color), as.character(clarity))

  expect_identical(nrow(R), nrow(S))
  for (i in seq_len(nrow(R))) {
    lhs <- R$model[[i]]
    rhs <- S$model[[i]]
    expect_equal(lhs$coefficients, rhs$coefficients)
  }

})

test_that("ml routines handle 'data' argument with 'do'", {
  test_requires("dplyr")

  S <- diamonds_tbl %>%
    group_by(color) %>%
    do(model = ml_linear_regression(price ~ x + y + z, data = .))

})
