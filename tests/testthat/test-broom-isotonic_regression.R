skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("isotonic_regression.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  ir_model <- iris_tbl %>%
    ml_isotonic_regression(Petal_Length ~ Petal_Width)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(ir_model)

  check_tidy(td1,
    exp.row = 44,
    exp.names = c("boundaries", "predictions")
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- ir_model %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1,
    exp.row = 150,
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".prediction")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(ir_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("isotonic", "num_boundaries")
  )
})
