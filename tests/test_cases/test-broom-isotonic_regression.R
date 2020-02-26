context("broom-isotonic_regression")

test_that("isotonic_regression.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  td1 <- iris_tbl %>%
    ml_isotonic_regression(Petal_Length ~ Petal_Width) %>%
    tidy()

  check_tidy(td1, exp.row = 44,
             exp.names = c("boundaries", "predictions"))

})

test_that("isotonic_regression.augment() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  au1 <- iris_tbl %>%
    ml_isotonic_regression(Petal_Length ~ Petal_Width) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 150,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".prediction"))
})

test_that("isotonic_regression.glance() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    ml_isotonic_regression(Petal_Length ~ Petal_Width) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("isotonic", "num_boundaries"))
})
