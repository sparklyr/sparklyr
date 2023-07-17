skip_connection("broom-logistic_regression")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()

test_that("logistic_regression tidiers work", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  iris_tbl <- testthat_tbl("iris") %>%
    filter(Species != "setosa")

  iris_local <- iris %>%
    filter(Species != "setosa")

  # Fitting model using sparklyr api
  lr_model <- iris_tbl %>%
    ml_logistic_regression(Species ~ Sepal_Length + Sepal_Width, family = "binomial")

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(lr_model)

  check_tidy(td1,
    exp.row = 3, exp.col = 2,
    exp.names = c("features", "coefficients")
  )

  expect_equal(
    td1$coefficients,
    c(-13.0460, 1.9024, 0.4047),
    tolerance = 0.01, scale = 1
    )

  ## --------------------------- augment() -------------------------------------

  # without newdata
  au1 <- collect(augment(lr_model))

  check_tidy(au1,
    exp.row = nrow(iris_local),
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  # with newdata
  au2 <- collect(augment(lr_model, newdata = iris_tbl))

  check_tidy(au2,
             exp.row = nrow(iris_local),
             exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  ## ---------------------------- glance() -------------------------------------
  gu1 <- glance(lr_model)

  check_tidy(gu1,
    exp.row = 1,
    exp.names = c("elastic_net_param", "lambda")
  )

  lr_parsnip <- parsnip::logistic_reg(engine = "spark") %>%
    parsnip::fit(Species ~ Sepal_Length + Sepal_Width, iris_tbl)

  expect_true(all(tidy(lr_parsnip) == td1))

  expect_true(all(collect(augment(lr_parsnip)) == au1))

  expect_true(all(collect(augment(lr_parsnip, new_data = iris_tbl)) == au2))

  expect_error(augment(lr_parsnip, newdata = iris_tbl))

  expect_true(all(glance(lr_parsnip) == gu1))
})
