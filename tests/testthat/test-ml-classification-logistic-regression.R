context("ml classification - logistic regression")
test_requires("dplyr")

sc <- testthat_spark_connection()

training <- data_frame(
  id = 0:3L,
  text = c("a b c d e spark",
           "b d",
           "spark f g h",
           "hadoop mapreduce"),
  label = c(1, 0, 1, 0)
)

training_tbl <- testthat_tbl("training")

test <- data_frame(
  id = 4:7L,
  text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
)
test_tbl <- testthat_tbl("test")

set.seed(42)
iris_weighted <- iris %>%
  dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                ones = rep(1, nrow(iris)),
                versicolor = ifelse(Species == "versicolor", 1L, 0L),
                not_versicolor = ifelse(Species == "versicolor", 0L, 1L))

iris_weighted_tbl <- testthat_tbl("iris_weighted")

test_that("ml_logistic_regression interprets params apporpriately", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, elastic_net_param = 0)
  expected_params <- list(intercept = TRUE, elastic_net_param = 0)
  params <- ml_param_map(lr)
  expect_equal(setdiff(expected_params, params), list())
})

test_that("ml_logistic_regression.spark_connect() returns object with correct class", {
  lr <- ml_logistic_regression(sc, intercept = TRUE, elastic_net_param = 0)
  expect_equal(class(lr), c("ml_logistic_regression", "ml_predictor", "ml_estimator",
                            "ml_pipeline_stage"))
})

test_that("ml_logistic_regression() does input checking", {
  expect_error(ml_logistic_regression(sc, elastic_net_param = "foo"),
               "length-one numeric vector")
  expect_equal(ml_logistic_regression(sc, max_iter = 25) %>%
                 ml_param("max_iter"),
               25L)
})

test_that("ml_logistic_regression.tbl_spark() works properly", {
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
    dplyr::pull(probability)

  m2 <- training_tbl %>%
    ft_tokenizer("text", "words") %>%
    ft_hashing_tf("words", "features", num_features = 1000) %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001)
  m2_predictions <- m2 %>%
    ml_transform(test_tbl %>%
                   ft_tokenizer("text", "words") %>%
                   ft_hashing_tf("words", "features", num_features = 1000)) %>%
    dplyr::pull(probability)

  expect_equal(m1_predictions, m2_predictions)
  expect_identical(class(m2), c("ml_logistic_regression_model", "ml_prediction_model",
                                "ml_transformer", "ml_pipeline_stage"))
})

test_that("ml_logistic_regression() agrees with stats::glm()", {

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

test_that("ml_logistic_regression() agrees with stats::glm() for reversed categories", {

  r <- glm(not_versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), weights = weights,
           data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "not_versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "weights")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)

  r <- glm(not_versicolor ~ Sepal.Width + Petal.Length + Petal.Width,
           family = binomial(logit), data = iris_weighted)
  s <- ml_logistic_regression(iris_weighted_tbl,
                              formula = "not_versicolor ~ Sepal_Width + Petal_Length + Petal_Width",
                              reg_param = 0L,
                              weight_col = "ones")
  expect_equal(unname(coef(r)), unname(coef(s)), tolerance = 1e-5)
})


test_that("ml_logistic_regression.tbl_spark() takes both quoted and unquoted formulas", {

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

test_that("ml_logistic_regression.tbl_spark() takes 'response' and 'features'
          columns instead of formula for backwards compatibility", {
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

test_that("ml_logistic_regression.tbl_spark() warns when 'response' is a formula and
          'features' is specified", {
            expect_warning(
              ml_logistic_regression(iris_weighted_tbl, response = versicolor ~ Sepal_Width + Petal_Length + Petal_Width,
                                     features = c("Sepal_Width", "Petal_Length", "Petal_Width")),
              "'features' is ignored when a formula is specified"
            )
          })

test_that("ml_logistic_regression.tbl_spark() errors if 'formula' is specified and either
          'response' or 'features' is specified", {
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


test_that("ml_logistic_regression parameter setting/getting works", {
  args <- list(
    x = sc,
    elastic_net_param = 0.1,
    features_col = "fcol",
    fit_intercept = FALSE,
    label_col = "lcol",
    max_iter = 50L,
    threshold = 0.4,
    tol = 1e-05,
    weight_col = "wcol",
    prediction_col = "pcol",
    probability_col = "probcol",
    raw_prediction_col = "rpcol")

  if (spark_version(sc) >= "2.1.0")
    args <- c(args, aggregation_depth = 3, family = "binomial")

  lr <- do.call(ml_logistic_regression, args)

  expect_equal(
    ml_params(lr, names(args)[-1]),
    args[-1])
})

test_that("logistic regression default params are correct", {
  lr <- ml_pipeline(sc) %>%
    ml_logistic_regression() %>%
    ml_stage(1)

  args <- get_default_args(
    ml_logistic_regression,
    c("x", "uid", "...", "thresholds", "weight_col",
      "lower_bounds_on_coefficients", "upper_bounds_on_coefficients",
      "lower_bounds_on_intercepts", "upper_bounds_on_intercepts"))

  if (spark_version(sc) < "2.1.0")
    args <- rlang::modify(args, aggregation_depth = NULL, family = NULL)

  expect_equal(
    ml_params(lr, names(args)),
    args)
})

test_that("we can fit multinomial models", {
  if (spark_version(sc) < "2.1.0") skip("multinomial models not supported < 2.1.0")
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
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
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
