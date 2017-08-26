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

