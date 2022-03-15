
skip_databricks_connect()

test_that("logistic_regression tidiers work", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris") %>%
  #iris_tbl <- sdf_copy_to(sc, iris) %>%
    dplyr::mutate(is_setosa = ifelse(Species == "setosa", 1, 0))

  # Fitting model using sparklyr api
  lr_model <- ml_logistic_regression(iris_tbl, is_setosa ~ Sepal_Length + Petal_Length)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(lr_model)

  check_tidy(td1,
    exp.row = 3, exp.col = 2,
    exp.names = c("features", "coefficients")
  )

  expect_equal(
    td1$coefficients,
    c(7.374718, 11.633918, -26.526868),
    tolerance = 0.01, scale = 1
    )

  ## --------------------------- augment() -------------------------------------

  # without newdata
  au1 <- collect(augment(lr_model))

  check_tidy(au1,
    exp.row = nrow(iris),
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".prediction")
  )

  # with newdata
  au2 <- collect(augment(lr_model, newdata = iris_tbl))

  check_tidy(au2,
             exp.row = nrow(iris),
             exp.name = c(dplyr::tbl_vars(iris_tbl), ".prediction")
  )

  ## ---------------------------- glance() -------------------------------------
  gu1 <- glance(lr_model)

  check_tidy(gu1,
    exp.row = 1,
    exp.names = c("elastic_net_param", "lambda")
  )


  skip("Preventing `parsnip` tests from running due to current bug")

  lr_parsnip <- parsnip::logistic_reg(engine = "spark") %>%
    parsnip::fit(is_setosa ~ Sepal_Length + Petal_Length, iris_tbl)

  expect_true(all(tidy(lr_parsnip) == td1))

  expect_true(all(collect(augment(lr_parsnip)) == au1))

  expect_true(all(collect(augment(lr_parsnip, new_data = iris_tbl)) == au2))

  expect_error(augment(lr_parsnip, newdata = iris_tbl))

  expect_true(all(glance(lr_parsnip) == gu1))
})
