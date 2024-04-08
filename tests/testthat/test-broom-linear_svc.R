skip_connection("broom-linear_svc")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("broom interface for Linear SVC works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  svc_model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_linear_svc(Species ~ ., reg_param = 0.01, max_iter = 10)


  td1 <- tidy(svc_model)

  ## ----------------------------- tidy() --------------------------------------

  expected_coefs <- c(-0.06004978, -0.1563083, -0.460648, 0.2276626, 1.055085)
  if(spark_version(sc) >= "3.2.0") expected_coefs <- c(-6.8823988, -0.6154984, -1.5135447, 1.9694126, 3.3736856)

  check_tidy(td1,
    exp.row = 5, exp.col = 2,
    exp.names = c("features", "coefficients")
  )
  expect_equal(td1$coefficients, expected_coefs,
    tolerance = 0.01,
    scale = 1
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

test_clear_cache()

