context("broom-decision_tree")

skip_databricks_connect()
test_that("decision_tree.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  dt_classification <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Length + Petal_Length, seed = 123)

  dt_regression <- iris_tbl %>%
    ml_decision_tree(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123)

  # for classification
  td1 <- tidy(dt_classification)

  check_tidy(td1,
    exp.row = 2,
    exp.names = c("feature", "importance")
  )
  expect_equal(td1$importance, c(0.94, 0.0603), tolerance = 0.001, scale = 1)

  # for regression
  td2 <- tidy(dt_regression)

  check_tidy(td2,
    exp.row = 2,
    exp.names = c("feature", "importance")
  )
  expect_equal(td2$importance, c(0.954, 0.0456), tolerance = 0.001, scale = 1)

  # for classification without newdata
  au1 <-  collect(augment(dt_classification))

  check_tidy(au1,
    exp.row = nrow(iris),
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".predicted_label"
    )
  )

  # for regression with newdata

  top_25 <- iris_tbl %>%
    head(25)

  au2 <-collect(augment(dt_regression, top_25))

  check_tidy(au2,
    exp.row = 25,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".prediction"
    )
  )

  # for classification
  gl1 <- glance(dt_classification)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("num_nodes", "depth", "impurity")
  )

  # for regression
  gl2 <- glance(dt_regression)

  check_tidy(gl2,
    exp.row = 1,
    exp.names = c("num_nodes", "depth", "impurity")
  )
})
