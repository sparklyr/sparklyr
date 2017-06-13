context("logistic regression")

sc <- testthat_spark_connection()

test_that("we can fit multinomial models", {
  test_requires("nnet", "dplyr")

  n <- 200
  data <- data.frame(
    x = seq_len(n),
    y = rep.int(letters[1:4], times = n / 4)
  )

  # fit multinomial model with R (suppress output for tests)
  capture.output(r <- nnet::multinom(y ~ x, data = data))

  # fit multinomial model with Spark
  tbl <- copy_to(sc, data, overwrite = TRUE)
  s <- ml_logistic_regression(y ~ x, data = tbl)

  # validate that they generate conforming predictions
  # (it seems their parameterizations are different so
  # the underlying models aren't identical, but we should
  # at least confirm they produce conforming predictions)
  train <- data.frame(x = sample(n))

  rp <- predict(r, train)
  sp <- predict(s, copy_to(sc, train, overwrite = TRUE))

  expect_equal(as.character(rp), as.character(sp))

})
