context("ml classification - naive bayes")

sc <- testthat_spark_connection()

test_that("ml_naive_bayes() parses params correctly", {
  args <- list(
    x = sc, label_col = "col", features_col = "fcol", prediction_col = "pcol",
    probability_col = "prcol", raw_prediction_col = "rpcol",
    model_type = "bernoulli", smoothing = 0.6, thresholds = c(0.2, 0.4, 0.6)
  )
  if (spark_version(sc) >= "2.1.0")
    args <- c(args, weight_col = "wcol")
  nb <- do.call(ml_naive_bayes, args)
  expect_equal(ml_params(nb, names(args)[-1]), args[-1])
})

test_that("ml_naive_bayes() default params are correct", {
  predictor <- ml_pipeline(sc) %>%
    ml_naive_bayes(layers = c(2, 2)) %>%
    ml_stage(1)

  args <- get_default_args(ml_naive_bayes,
                           c("x", "uid", "...", "thresholds", "weight_col"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

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
    na.omit() %>%
    ml_naive_bayes(Class ~ .)

  Rp <- as.numeric(R$apriori / sum(R$apriori))
  Sp <- as.numeric(exp(model[["pi"]]))

  expect_equal(Rp, Sp, tolerance = 0.001)
})
