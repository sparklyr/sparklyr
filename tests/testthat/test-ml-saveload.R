context("saveload")
sc <- testthat_spark_connection()

test_that("we can save + load a RandomForest regression model", {
  skip_on_cran()

  mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
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
