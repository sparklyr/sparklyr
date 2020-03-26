context("broom-logistic_regression")

test_that("logistic_regression.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for multiclass classification
  td1 <- iris_tbl %>%
    ml_logistic_regression(Species ~ Sepal_Length + Petal_Length) %>%
    tidy()

  check_tidy(td1, exp.row = 3, exp.col = 4,
             exp.names = c("features", "versicolor_coef",
                           "virginica_coef", "setosa_coef"))
  expect_equal(td1$versicolor_coef, c(15.26, -5.07, 7.7), tolerance = 0.001)

  # for binary classification
  td2 <- iris_tbl %>%
    dplyr::filter(Species != "setosa") %>%
    ml_logistic_regression(Species ~ Sepal_Length + Petal_Length) %>%
    tidy()

  check_tidy(td2, exp.row = 3, exp.col = 2,
             exp.names = c("features", "coefficients"))
  expect_equal(td2$coefficients, c(-39.8, -4.02, 13.3), tolerance = 0.001)

})

test_that("logistic_regression.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # without newdata
  au1 <- iris_tbl %>%
    ml_logistic_regression(Species ~ Sepal_Length + Petal_Length) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = nrow(iris),
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))


  # with newdata
  au2 <- iris_tbl %>%
    ml_logistic_regression(Species ~ Sepal_Length + Petal_Length) %>%
    augment(newdata = head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))
})

test_that("logistic_regression.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    ml_logistic_regression(Species ~ Sepal_Length + Petal_Length) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("elastic_net_param", "lambda"))

})
