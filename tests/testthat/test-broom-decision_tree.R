context("broom-decision_tree")

test_that("decision_tree.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for classification
  td1 <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    tidy()

  check_tidy(td1, exp.row = 2,
             exp.names = c("feature", "importance"))
  expect_equal(td1$importance, c(0.94, 0.0603), tolerance = 0.001)

  # for regression
  td2 <- iris_tbl %>%
    ml_decision_tree(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    tidy()

  check_tidy(td2, exp.row = 2,
             exp.names = c("feature", "importance"))
  expect_equal(td2$importance, c(0.954, 0.0456), tolerance = 0.001)

})

test_that("decision_tree.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # for classification without newdata
  au1 <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = nrow(iris),
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))


  # for regression with newdata
  au2 <- iris_tbl %>%
    ml_decision_tree(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".prediction"))
})

test_that("decision_tree.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # for classification
  gl1 <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("num_nodes", "depth", "impurity"))

  # for regression
  gl2 <- iris_tbl %>%
    ml_decision_tree(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    glance()

  check_tidy(gl2, exp.row = 1,
             exp.names = c("num_nodes", "depth", "impurity"))

})
