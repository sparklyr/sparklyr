context("ml print methods")

sc <- testthat_spark_connection()

test_that("printing works for ml_model_logistic_regression", {
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  s <- ml_logistic_regression(
    iris_weighted_tbl,
    response = "versicolor",
    features = c("Sepal_Width", "Petal_Length", "Petal_Width"))

  expect_output(print(s),
                "Call: ml_logistic_regression.tbl_spark\\(")

})

