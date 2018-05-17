context("broom")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("tidy.{glm type models} works", {
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  glmfit1 <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg", features = "wt")
  td1 <- tidy(glmfit1)
  check_tidy(td1, exp.row = 2,
             exp.names = c("term", "estimate", "std.error", "statistic", "p.value"))
  expect_equal(td1$term, c("(Intercept)", "wt"))

  glmfit2 <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg",
                                              features = c("wt", "disp"))
  expect_warning(
    td2 <- tidy(glmfit2, exponentiate = TRUE)
  )
  check_tidy(td2, exp.row = 3,
             exp.names = c("term", "estimate", "statistic", "p.value"))
  expect_equal(td2$term, c("(Intercept)", "wt", "disp"))

  glmfit1_r <- glm(mpg ~ wt, mtcars, family = "gaussian")
  expect_equal(broom::tidy(glmfit1_r), broom::tidy(glmfit1))

  lmfit1 <- ml_linear_regression(mtcars_tbl, "mpg ~ wt")
  td1 <- tidy(lmfit1)
  check_tidy(td1, exp.row = 2,
             exp.names = c("estimate", "std.error", "statistic", "p.value"))
  expect_equal(td1$term, c("(Intercept)", "wt"))

  lmfit2 <- ml_linear_regression(mtcars_tbl, mpg ~ wt + disp)
  td2 <- tidy(lmfit2)
  check_tidy(td2, exp.row = 3,
             exp.names = c("estimate", "std.error", "statistic", "p.value"))
  expect_equal(td2$term, c("(Intercept)", "wt", "disp"))

  lmfit1_r <- lm(mpg ~ wt, mtcars)
  expect_equal(broom::tidy(lmfit1_r), broom::tidy(lmfit1))
})

test_that("augment.{glm type models} works", {
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  glmfit <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg",
                                             features = "wt")
  au1 <- augment(glmfit) %>% collect()
  check_tidy(au1, exp.row = nrow(mtcars),
             exp.name = c(names(mtcars), "fitted", "resid"))

  new_data <- mtcars_tbl %>% head(10)
  au2 <- augment(glmfit, newdata = new_data) %>% collect()
  check_tidy(au2, exp.row = 10,
             exp.name = c(names(mtcars), "fitted", "resid"))

  expect_error(augment(glmfit, newdata = new_data,
                       type.residuals = "deviance"),
               "'type.residuals' must be set to 'working' when 'newdata' is supplied")
})

test_that("augment (ml glm) working residuals agree with residuals()", {
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  glmfit <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg",
features = "wt")
  au <- augment(glmfit) %>% collect()
  expect_equal(au$resid,
               residuals(glmfit, type = "working"))
})

test_that("glance.{glm type models} works", {
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  glmfit <- ml_generalized_linear_regression(mtcars_tbl, response = "mpg",
features = "wt")
  check_tidy(glance(glmfit), exp.row = 1,
             exp.names = c("null.deviance", "df.null", "AIC", "deviance", "df.residual"))

  lmfit <- ml_linear_regression(mtcars_tbl, "mpg ~ wt")
  check_tidy(glance(lmfit), exp.row = 1,
             exp.names = c("explained.variance", "mean.absolute.error", "mean.squared.error",
                           "r.squared", "root.mean.squared.error"))
})

