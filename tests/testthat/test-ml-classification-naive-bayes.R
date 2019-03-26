context("ml classification - naive bayes")

test_that("ml_naive_bayes() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_naive_bayes)
})

test_that("ml_naive_bayes() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    label_col = "col",
    features_col = "fcol",
    prediction_col = "pcol",
    probability_col = "prcol",
    raw_prediction_col = "rpcol",
    model_type = "bernoulli",
    smoothing = 0.6,
    thresholds = c(0.2, 0.4, 0.6)
  )
  test_param_setting(sc, ml_naive_bayes, test_args)
})

test_that("ml_naive_bayes() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "accuracy metric support")
  sample_data_path <- dir(getwd(), recursive = TRUE, pattern = "sample_libsvm_data.txt", full.names = TRUE)

  sample_data <- spark_read_libsvm(sc, "sample_data",
                                   sample_data_path, overwrite = TRUE)

  sample_data_partitioned <- sample_data %>%
    sdf_partition(weights = c(train = 0.7, test = 0.3), seed = 1)

  model <- ml_naive_bayes(sample_data_partitioned$train)

  predictions <- model %>%
    ml_predict(sample_data_partitioned$test)

  expect_equal(ml_multiclass_classification_evaluator(
    predictions, label_col = "label", prediction_col = "prediction",
    metric_name = "accuracy"),
    1.0)
})

test_that("ml_naive_bayes() and e1071::naiveBayes produce similar results", {
  sc <- testthat_spark_connection()
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

test_that("ml_naive_bayes() print outputs are correct", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  m <- ml_naive_bayes(iris_tbl, Species ~ Petal_Width + Petal_Length)
  expect_output_file(
    print(m),
    output_file("print/naive_bayes.txt")
  )
  expect_output_file(
    summary(m),
    output_file("print/naive_bayes.txt"), update = FALSE
  )
})
