
skip_databricks_connect()
test_that("linear_svc.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  svc_model <- iris_tbl %>%
    dplyr::filter(Species != "setosa") %>%
    ml_linear_svc(Species ~ .)


  td1 <- tidy(svc_model)

  ## ----------------------------- tidy() --------------------------------------

  check_tidy(td1,
    exp.row = 5, exp.col = 2,
    exp.names = c("features", "coefficients")
  )
  expect_equal(td1$coefficients, c(-4.55, -0.981, -2.85, 1.87, 6.06),
    tolerance = 0.01, scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- svc_model %>%
    augment() %>%
    collect()

  check_tidy(au1,
    exp.row = 100,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".predicted_label"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(svc_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("reg_param", "standardization", "aggregation_depth")
  )
})
