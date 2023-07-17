skip_connection("broom-decision_tree")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("decision_tree.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  dt_classification <- iris_tbl %>%
    ml_decision_tree(Species ~ Sepal_Length + Petal_Length, seed = 123)

  dt_regression <- iris_tbl %>%
    ml_decision_tree(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  # for classification
  td1 <- tidy(dt_classification)

  check_tidy(td1, exp.row = 2, exp.names = c("feature", "importance"))

  expect_equal(td1$importance, c(0.94, 0.0603), tolerance = 0.001, scale = 1)



  # for regression
  td2 <- tidy(dt_regression)

  check_tidy(td2, exp.row = 2, exp.names = c("feature", "importance"))

  expect_equal(td2$importance, c(0.954, 0.0456), tolerance = 0.001, scale = 1)



  ## --------------------------- augment() -------------------------------------

  iris_vars <- dplyr::tbl_vars(iris_tbl)

  # for classification without newdata
  au1 <-  collect(augment(dt_classification))

  check_tidy(au1,
             exp.row = nrow(iris),
             exp.name = c(iris_vars, ".predicted_label")
             )

  # for regression with newdata

  top_25 <- iris_tbl %>%
    head(25)

  au2 <-collect(augment(dt_regression, top_25))

  check_tidy(au2,
             exp.row = 25,
             exp.name = c(iris_vars, ".prediction")
             )


  ## ---------------------------- glance() -------------------------------------

  gl_names <- c("num_nodes", "depth", "impurity")

  # for classification
  gl1 <- glance(dt_classification)

  check_tidy(gl1, exp.row = 1, exp.names = gl_names)



  # for regression
  gl2 <- glance(dt_regression)

  check_tidy(gl2,
    exp.row = 1,
    exp.names = gl_names
  )

  dt_classification_parsnip <- parsnip::decision_tree(engine = "spark") %>%
    parsnip::set_mode("classification") %>%
    parsnip::fit(Species ~ Sepal_Length + Petal_Length, iris_tbl)

  dt_regression_parsnip <- parsnip::decision_tree(engine = "spark") %>%
    parsnip::set_mode("regression") %>%
    parsnip::fit(Sepal_Length ~ Petal_Length + Petal_Width, iris_tbl)

  expect_true(
    all(collect(augment(dt_classification_parsnip)) == au1)
  )

  expect_true(
    all(collect(augment(dt_regression_parsnip, top_25)) == au2)
  )

  expect_true(
    all(glance(dt_classification_parsnip) == gl1)
  )

  expect_true(
    all(glance(dt_regression_parsnip) == gl2)
  )

  expect_true(
    all(tidy(dt_classification_parsnip) == td1)
  )

  expect_true(
    all(tidy(dt_regression_parsnip) == td2)
  )



})

