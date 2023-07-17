skip_connection("broom")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("tidy.{glm type models} works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires("broom")
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  glmfit1 <- ml_generalized_linear_regression(
    mtcars_tbl,
    response = "mpg",
    features = "wt"
    )

  glmfit2 <- ml_generalized_linear_regression(
    mtcars_tbl,
    response = "mpg",
    features = c("wt", "disp")
  )

  lmfit1 <- ml_linear_regression(mtcars_tbl, "mpg ~ wt")

  lmfit2 <- ml_linear_regression(mtcars_tbl, mpg ~ wt + disp)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(glmfit1)

  check_tidy(td1,
    exp.row = 2,
    exp.names = c("term", "estimate", "std.error", "statistic", "p.value")
  )

  expect_equal(td1$term, c("(Intercept)", "wt"))

  expect_warning(
    td2 <- tidy(glmfit2, exponentiate = TRUE)
  )

  check_tidy(td2,
    exp.row = 3,
    exp.names = c("term", "estimate", "statistic", "p.value")
  )

  expect_equal(td2$term, c("(Intercept)", "wt", "disp"))


  glmfit1_r <- glm(mpg ~ wt, mtcars, family = "gaussian")

  expect_equal(
    as.data.frame(tidy(glmfit1_r)),
    as.data.frame(tidy(glmfit1))
    )

  td1 <- tidy(lmfit1)

  check_tidy(td1,
    exp.row = 2,
    exp.names = c("estimate", "std.error", "statistic", "p.value")
  )

  expect_equal(td1$term, c("(Intercept)", "wt"))

  td2 <- tidy(lmfit2)

  check_tidy(td2,
    exp.row = 3,
    exp.names = c("estimate", "std.error", "statistic", "p.value")
  )

  expect_equal(td2$term, c("(Intercept)", "wt", "disp"))

  lmfit1_r <- lm(mpg ~ wt, mtcars)

  expect_equal(
    as.data.frame(tidy(lmfit1_r)),
    as.data.frame(tidy(lmfit1))
    )

  ## --------------------------- augment() -------------------------------------

  au1 <- augment(glmfit1) %>% collect()

  check_tidy(au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), "fitted", "resid")
  )

  new_data <- mtcars_tbl %>% head(10)

  au2 <- augment(glmfit1, newdata = new_data) %>% collect()
  check_tidy(au2,
    exp.row = 10,
    exp.name = c(names(mtcars), "fitted", "resid")
  )

  expect_error(
    augment(glmfit1,
      newdata = new_data,
      type.residuals = "deviance"
    ),
    "'type.residuals' must be set to 'working' when 'newdata' is supplied"
  )

  au <- augment(glmfit1) %>%
    collect()

  expect_equal(
    au$resid,
    residuals(glmfit1, type = "working")
  )

  ## ---------------------------- glance() -------------------------------------

  check_tidy(
    glance(glmfit1),
    exp.row = 1,
    exp.names = c("null.deviance", "df.null", "AIC", "deviance", "df.residual")
  )

  check_tidy(
    glance(lmfit1),
    exp.row = 1,
    exp.names = c(
      "explained.variance", "mean.absolute.error", "mean.squared.error",
      "r.squared", "root.mean.squared.error"
    )
  )
})
