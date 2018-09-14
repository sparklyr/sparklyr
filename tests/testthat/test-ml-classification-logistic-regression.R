context("ml classification - logistic regression")

test_that("ml_logistic_regression() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_logistic_regression)
})

test_that("ml_logistic_regression() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    fit_intercept = FALSE,
    elastic_net_param = 1e-4,
    reg_param = 1e-5,
    max_iter = 50,
    # `threshold` can't seem to be set when `thresholds` is
    thresholds = c(0.3, 0.7),
    tol = 1e-04,
    weight_col = "wow",
    aggregation_depth = 3,
    # We'll want to enable this, see #1616
    # upper_bounds_on_coefficients = matrix(rep(1, 6), nrow = 3),
    # lower_bounds_on_coefficients = matrix(rep(-1, 6), nrow = 3),
    # upper_bounds_on_intercepts = c(1, 1, 1),
    # lower_bounds_on_intercepts = c(-1, -1, -1),
    features_col = "foo",
    label_col = "bar",
    family = "multinomial",
    prediction_col = "pppppp",
    probability_col = "apweiof",
    raw_prediction_col = "rparprpr"
  )
  test_param_setting(sc, ml_logistic_regression, test_args)
})

test_that("ml_logistic_regression.tbl_spark() works properly", {
  sc <- testthat_spark_connection()

  training <- data_frame(
    id = 0:3L,
    text = c("a b c d e spark",
             "b d",
             "spark f g h",
             "hadoop mapreduce"),
    label = c(1, 0, 1, 0)
  )
  test <- data_frame(
    id = 4:7L,
    text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
  )
  training_tbl <- testthat_tbl("training")
  test_tbl <- testthat_tbl("test")

  pipeline <- ml_pipeline(sc) %>%
    ft_tokenizer("text", "words") %>%
    ft_hashing_tf("words", "features", num_features = 1000) %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001)

  m1 <- pipeline %>%
    ml_fit(training_tbl)
  m1_predictions <- m1 %>%
    ml_transform(test_tbl) %>%
    pull(probability)

  m2 <- training_tbl %>%
    ft_tokenizer("text", "words") %>%
    ft_hashing_tf("words", "features", num_features = 1000) %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001)
  m2_predictions <- m2 %>%
    ml_transform(test_tbl %>%
                   ft_tokenizer("text", "words") %>%
                   ft_hashing_tf("words", "features", num_features = 1000)) %>%
    pull(probability)

  expect_equal(m1_predictions, m2_predictions)
  expect_identical(class(m2), c("ml_logistic_regression_model", "ml_prediction_model",
                                "ml_transformer", "ml_pipeline_stage"))
})

test_that("ml_logistic_regression() agrees with stats::glm()", {
  sc <- testthat_spark_connection()
  set.seed(42)
  iris_weighted <- iris %>%
    mutate(weights = rpois(nrow(iris), 1) + 1,
           ones = rep(1, nrow(iris)),
           versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), weights = weights,
           data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)
})

test_that("ml_logistic_regression can fit without intercept",{
  sc <- testthat_spark_connection()
  set.seed(42)
  iris_weighted <- iris %>%
    mutate(weights = rpois(nrow(iris), 1) + 1,
           ones = rep(1, nrow(iris)),
           versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")
  expect_error(s <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = versicolor ~ Sepal_Width + Petal_Length + Petal_Width,
    fit_intercept=FALSE),NA)
  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width - 1, family=binomial(logit), data=iris_weighted)
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)
})

test_that("ml_logistic_regression() agrees with stats::glm() for reversed categories", {
  sc <- testthat_spark_connection()
  set.seed(42)
  iris_weighted <- iris %>%
    mutate(weights = rpois(nrow(iris), 1) + 1,
           ones = rep(1, nrow(iris)),
           versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), weights = weights,
           data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)
})

test_that("ml_logistic_regression.tbl_spark() takes both quoted and unquoted formulas", {
  sc <- testthat_spark_connection()
  iris_weighted_tbl <- testthat_tbl("iris_weighted")
  m1 <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width"
  )

  m2 <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = versicolor ~ Sepal_Width + Petal_Length + Petal_Width
  )

  expect_identical(m1$formula, m2$formula)
})

test_that("ml_logistic_regression.tbl_spark() takes 'response' and 'features' columns instead of formula for backwards compatibility", {
  sc <- testthat_spark_connection()
  iris_weighted_tbl <- testthat_tbl("iris_weighted")
  m1 <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width"
  )

  m2 <- ml_logistic_regression(
    iris_weighted_tbl,
    response = "versicolor",
    features = c("Sepal_Width", "Petal_Length", "Petal_Width")
  )

  expect_identical(m1$formula, m2$formula)
})

test_that("ml_logistic_regression.tbl_spark() warns when 'response' is a formula and 'features' is specified", {
  sc <- testthat_spark_connection()
  iris_weighted_tbl <- testthat_tbl("iris_weighted")
  expect_warning(
    ml_logistic_regression(iris_weighted_tbl, response = versicolor ~ Sepal_Width + Petal_Length + Petal_Width,
                           features = c("Sepal_Width", "Petal_Length", "Petal_Width")),
    "'features' is ignored when a formula is specified"
  )
})

test_that("ml_logistic_regression.tbl_spark() errors if 'formula' is specified and either 'response' or 'features' is specified", {
  sc <- testthat_spark_connection()
  iris_weighted_tbl <- testthat_tbl("iris_weighted")
  expect_error(
    ml_logistic_regression(iris_weighted_tbl,
                           "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                           response = "versicolor"),
    "only one of 'formula' or 'response'-'features' should be specified"
  )
  expect_error(
    ml_logistic_regression(iris_weighted_tbl,
                           "versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                           features = c("Sepal_Width", "Petal_Length", "Petal_Width")),
    "only one of 'formula' or 'response'-'features' should be specified"
  )
})

test_that("we can fit multinomial models", {
  sc <- testthat_spark_connection()
  test_requires_version("2.1.0", "multinomial models not supported < 2.1.0")

  n <- 200
  data <- data.frame(
    x = seq_len(n),
    y = rep.int(letters[1:4], times = n / 4)
  )

  # fit multinomial model with R (suppress output for tests)
  capture.output(r <- nnet::multinom(y ~ x, data = data))

  # fit multinomial model with Spark
  tbl <- copy_to(sc, data, overwrite = TRUE)
  s <- ml_logistic_regression(tbl, y ~ x)

  # validate that they generate conforming predictions
  # (it seems their parameterizations are different so
  # the underlying models aren't identical, but we should
  # at least confirm they produce conforming predictions)
  train <- data.frame(x = sample(n))

  rp <- predict(r, train)
  sp <- predict(s, copy_to(sc, train, overwrite = TRUE))

  expect_equal(as.character(rp), as.character(sp))

})

test_that("weights column works for logistic regression", {
  sc <- testthat_spark_connection()
  set.seed(42)
  iris_weighted <- iris %>%
    mutate(weights = rpois(nrow(iris), 1) + 1,
           ones = rep(1, nrow(iris)),
           versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), weights = weights,
           data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              response = "versicolor",
                              features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                              lambda = 0L,
                              weights.column = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)

  r <- glm(versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              response = "versicolor",
                              features = c("Sepal_Width", "Petal_Length", "Petal_Width"),
                              lambda = 0L,
                              weights.column = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)
})

test_that("logistic regression bounds on coefficients", {
  sc <- testthat_spark_connection()
  test_requires_version("2.2.0", "coefficient bounds require 2.2+")

  iris_tbl <- testthat_tbl("iris")
  lr <- ml_logistic_regression(
    iris_tbl, Species ~ Petal_Width + Sepal_Length,
    upper_bounds_on_coefficients = matrix(rep(1, 6), nrow = 3),
    lower_bounds_on_coefficients = matrix(rep(-1, 6), nrow = 3),
    upper_bounds_on_intercepts = c(1, 1, 1),
    lower_bounds_on_intercepts = c(-1, -1, -1))
  expect_equal(max(coef(lr)), 1)
  expect_equal(min(coef(lr)), -1)
})
