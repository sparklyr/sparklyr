context("broom-random_forest")

test_that("random_forest.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for classification
  td1 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    tidy()

  check_tidy(td1, exp.row = 2,
             exp.names = c("feature", "importance"))
  expect_equal(td1$importance, c(0.941, 0.0586), tolerance = 0.001)

  # for regression
  td2 <- iris_tbl %>%
    ml_random_forest(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    tidy()

  check_tidy(td2, exp.row = 2,
             exp.names = c("feature", "importance"))
  expect_equal(td2$importance, c(0.658, 0.342), tolerance = 0.001)

})

test_that("random_forest.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # for classification without newdata
  au1 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = nrow(iris),
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))


  # for regression with newdata
  au2 <- iris_tbl %>%
    ml_random_forest(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".prediction"))
})

test_that("random_forest.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # for classification
  gl1 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Length + Petal_Length, seed = 123) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("num_trees", "total_num_nodes",
                           "max_depth", "impurity",
                           "subsampling_rate"))

  # for regression
  gl2 <- iris_tbl %>%
    ml_random_forest(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123) %>%
    glance()

  check_tidy(gl2, exp.row = 1,
             exp.names = c("num_trees", "total_num_nodes",
                           "max_depth", "impurity",
                           "subsampling_rate"))

})
