skip_connection("tidiers_ml_regression")
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

  check_tidy(
    td1,
    exp.row = 2,
    exp.names = c("term", "estimate", "std.error", "statistic", "p.value")
  )

  expect_equal(td1$term, c("(Intercept)", "wt"))

  expect_warning(
    td2 <- tidy(glmfit2, exponentiate = TRUE)
  )

  check_tidy(
    td2,
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

  check_tidy(
    td1,
    exp.row = 2,
    exp.names = c("estimate", "std.error", "statistic", "p.value")
  )

  expect_equal(td1$term, c("(Intercept)", "wt"))

  td2 <- tidy(lmfit2)

  check_tidy(
    td2,
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

  check_tidy(
    au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), "fitted", "resid")
  )

  new_data <- mtcars_tbl %>% head(10)

  au2 <- augment(glmfit1, newdata = new_data) %>% collect()
  check_tidy(au2, exp.row = 10, exp.name = c(names(mtcars), "fitted", "resid"))

  expect_error(
    augment(glmfit1, newdata = new_data, type.residuals = "deviance"),
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
      "explained.variance",
      "mean.absolute.error",
      "mean.squared.error",
      "r.squared",
      "root.mean.squared.error"
    )
  )
})

test_that("aft_survival_regression.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  df <- data.frame(
    label = c(1.218, 2.949, 3.627, 0.273, 4.199),
    censor = c(1, 0, 0, 1, 0),
    a = c(1.560, 0.346, 1.380, 0.520, 0.795),
    b = c(-0.605, 2.158, 0.231, 1.151, -0.226)
  )

  df_tbl <- sdf_copy_to(sc, df, name = "df_tbl", overwrite = TRUE)

  aft_model <- df_tbl %>%
    ml_aft_survival_regression(label ~ a + b, censor = "censor")

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(aft_model)

  check_tidy(
    td1,
    exp.row = 3,
    exp.col = 2,
    exp.names = c("features", "coefficients")
  )
  expect_equal(
    td1$coefficients,
    c(2.64, -0.496, 0.198),
    tolerance = 0.001,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- augment(aft_model) %>%
    collect()

  check_tidy(
    au1,
    exp.row = 5,
    exp.name = c(
      dplyr::tbl_vars(df_tbl),
      ".prediction"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(aft_model)

  check_tidy(gl1, exp.row = 1, exp.names = c("scale", "aggregation_depth"))
})

test_that("isotonic_regression.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  ir_model <- iris_tbl %>%
    ml_isotonic_regression(Petal_Length ~ Petal_Width)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(ir_model)

  check_tidy(
    td1,
    exp.row = length(ir_model$model$boundaries()),
    exp.names = c("boundaries", "predictions")
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- ir_model %>%
    augment() %>%
    dplyr::collect()

  check_tidy(
    au1,
    exp.row = 150,
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".prediction")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(ir_model)

  check_tidy(gl1, exp.row = 1, exp.names = c("isotonic", "num_boundaries"))
})

test_clear_cache()
