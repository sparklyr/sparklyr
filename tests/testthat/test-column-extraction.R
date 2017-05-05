context("Column Separation")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")

test_that("we can interact with vector columns", {

  # fit a (nonsensical) logistic regression model
  model <- ml_logistic_regression(
    mtcars_tbl,
    response = "vs",
    features = "mpg"
  )

  # get predicted values
  fitted <- sdf_predict(model)

  # extract first element from 'probability' vector
  extracted <- sdf_separate_column(
    fitted,
    "probability",
    list(
      "P(x)" = 1
    )
  )

  # retrieve the columns
  probability <- extracted %>%
    sdf_read_column("probability") %>%
    lapply(function(el) {
      invoke(el, "toArray")
    })

  # split into pieces
  first <- as.numeric(lapply(probability, `[[`, 1L))
  second <- as.numeric(lapply(probability, `[[`, 2L))

  # verify we have the expected result
  expect_equal(first, sdf_read_column(extracted, "P(x)"))

  # now, try generating for each element
  splat <- sdf_separate_column(
    fitted,
    "probability",
    c("P(X)", "1 - P(X)")
  )

  # verify they're equal
  expect_equal(first, sdf_read_column(splat, "P(X)"))
  expect_equal(second, sdf_read_column(splat, "1 - P(X)"))

})
