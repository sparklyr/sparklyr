context("broom-naive_bayes")

test_that("naive_bayes.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for multiclass classification
  td1 <- iris_tbl %>%
    ml_naive_bayes(Species ~ Sepal_Length + Petal_Length) %>%
    tidy()

  check_tidy(td1, exp.row = 3, exp.col = 4,
             exp.names = c(".label", "Sepal_Length",
                           "Petal_Length", ".pi"))
  expect_equal(td1$Sepal_Length, c(-0.542, -0.612, -0.258), tolerance = 0.001)

})

test_that("naive_bayes.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # with newdata
  au1 <- iris_tbl %>%
    ml_naive_bayes(Species ~ Sepal_Length + Petal_Length) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))
})

test_that("naive_bayes.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    ml_naive_bayes(Species ~ Sepal_Length + Petal_Length) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("model_type", "smoothing"))
})
