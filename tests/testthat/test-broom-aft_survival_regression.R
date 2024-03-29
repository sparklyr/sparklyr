skip_connection("broom-aft_survival_regression")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
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

  check_tidy(td1,
    exp.row = 3, exp.col = 2,
    exp.names = c("features", "coefficients")
  )
  expect_equal(td1$coefficients, c(2.64, -0.496, 0.198),
    tolerance = 0.001, scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  au1 <-  augment(aft_model) %>%
    collect()

  check_tidy(au1,
    exp.row = 5,
    exp.name = c(
      dplyr::tbl_vars(df_tbl),
      ".prediction"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(aft_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("scale", "aggregation_depth")
  )
})

test_clear_cache()

