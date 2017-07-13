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
  skip_on_travis()

  mtcars_tbl <- testthat_tbl("mtcars")

  # pairs of read / write routines that should work together
  routines <- list(
    c(spark_write_table,   spark_read_table),
    c(spark_write_parquet, spark_read_parquet)
  )

  for (pair in routines) {
    writer <- pair[[1]]
    reader <- pair[[2]]

    name <- sparklyr:::random_string("")

    path <- name

    writer(mtcars_tbl, path)
    loaded_tbl <- reader(sc, name, path)

    expect_identical(collect(mtcars_tbl), collect(loaded_tbl))
  }
})

test_that("we can save / load models with custom metadata readers / writers", {
  skip_on_cran()

  # fit a simple model
  mtcars_tbl <- testthat_tbl("mtcars")
  model <- mtcars_tbl %>%
    ml_random_forest(mpg ~ cyl)

  # construct dummy save / load functions
  model_path <- tempfile()
  meta_path <- tempfile()
  meta_saver <- function(meta, file) saveRDS(meta, file = meta_path)
  meta_loader <- function(file) readRDS(meta_path)

  # use them
  saved <- ml_save(model, model_path, meta_saver)
  loaded <- ml_load(sc, model_path, meta_loader)

  # not really a full test but sufficient for now
  lhs <- capture.output(print(model))
  rhs <- capture.output(print(loaded))
  expect_identical(lhs, rhs)

})
