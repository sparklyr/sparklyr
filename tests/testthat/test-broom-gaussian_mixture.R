skip_connection("broom-gaussian_mixture")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("gaussian_mixture.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  model <- mtcars_tbl %>%
    ml_gaussian_mixture(~ mpg + cyl, k = 4L, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  expect_warning_on_arrow(
    td1 <- tidy(model)
  )

  check_tidy(td1,
             exp.row = 4,
             exp.names = c("mpg", "cyl", "weight", "size", "cluster")
  )

  expect_equal(td1$size, model$summary$cluster_sizes())

  ## --------------------------- augment() -------------------------------------

  au1 <- model %>%
    augment() %>%
    collect()

  check_tidy(au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), ".cluster")
  )

  au2 <- model %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    collect()

  check_tidy(au2,
    exp.row = 25,
    exp.name = c(names(mtcars), ".cluster")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(model)

  if (spark_version(sc) >= "2.3.0") {
    check_tidy(gl1,
      exp.row = 1,
      exp.names = c("k", "silhouette")
    )
  } else {
    check_tidy(gl1,
      exp.row = 1,
      exp.names = "k"
    )
  }
})

test_clear_cache()

