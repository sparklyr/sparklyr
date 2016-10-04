context("saveload")
sc <- testthat_spark_connection()

test_that("we can save + load a RandomForest regression model", {
  skip_on_cran()
  if (spark_version(sc) < "2.0.0")
    skip("requires Spark 2.0.0")

  mtcars_tbl <- testthat_tbl("mtcars")
  model <- mtcars_tbl %>%
    ml_random_forest(mpg ~ cyl)

  path <- tempfile()
  saved <- ml_save(model, path)
  loaded <- ml_load(sc, path)

  # not really a full test but sufficient for now
  lhs <- capture.output(print(model))
  rhs <- capture.output(print(loaded))
  expect_identical(lhs, rhs)
})

test_that("we can save + load tables using the various save/load APIs", {
  skip_on_cran()
  mtcars_tbl <- testthat_tbl("mtcars")

  # pairs of read / write routines that should work together
  routines <- list(
    c(sdf_save_table,   sdf_load_table),
    c(sdf_save_parquet, sdf_load_parquet)
  )

  for (pair in routines) {
    writer <- pair[[1]]
    reader <- pair[[2]]

    name <- sparklyr:::random_string("")
    writer(mtcars_tbl, name)
    loaded_tbl <- reader(sc, name)

    expect_identical(collect(mtcars_tbl), collect(loaded_tbl))
  }
})
