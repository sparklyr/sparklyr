context("naive bayes")

sc <- testthat_spark_connection()

test_that("ml_naive_bayes() and e1071::naiveBayes produce similar results", {
  skip_on_cran()
  test_requires("e1071", "mlbench")

  data("HouseVotes84", package = "mlbench")

  # transform factors to integer vectors
  data <- HouseVotes84
  data$Class <- as.character(data$Class)
  data[-1L] <- lapply(data[-1L], function(x) {
    as.integer(x) - 1
  })

  # compute R-side naive bayes model
  R <- naiveBayes(Class ~ ., data = data, na.action = na.omit)

  tbl <- dplyr::copy_to(sc, data, "HouseVotes84")

  # compute Spark-side naive bayes model
  model <- S <- tbl %>%
    ml_naive_bayes(Class ~ ., ml.options = ml_options(na.action = na.omit))

  Rp <- as.numeric(R$apriori / sum(R$apriori))
  Sp <- as.numeric(exp(model$pi))

  expect_equal(Rp, Sp)
})
