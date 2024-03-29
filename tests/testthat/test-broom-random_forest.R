skip_connection("broom-random_forest")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("random_forest.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  rf_classification <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Length + Petal_Length, seed = 123)

  rf_regression <- iris_tbl %>%
    ml_random_forest(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  # for classification
  td1 <- tidy(rf_classification)

  check_tidy(td1, exp.row = 2, exp.names = c("feature", "importance"))

  expect_equal(td1$importance, c(0.941, 0.0586), tolerance = 0.1, scale = 1)


  # for regression
  td2 <- tidy(rf_regression)

  check_tidy(td2, exp.row = 2, exp.names = c("feature", "importance"))

  expect_equal(td2$importance, c(0.658, 0.342), tolerance = 0.2, scale = 1)

  ## --------------------------- augment() -------------------------------------

  iris_vars <- dplyr::tbl_vars(iris_tbl)

  # for classification without newdata
  au1 <- collect(augment(rf_classification))

  check_tidy(au1,
    exp.row = nrow(iris),
    exp.name = c(iris_vars, ".predicted_label")
  )

  top_25 <- iris_tbl %>%
    head(25)

  au2 <- collect(augment(rf_regression, top_25))

  check_tidy(au2,
    exp.row = 25,
    exp.name = c(iris_vars, ".prediction")
  )


  ## ---------------------------- glance() -------------------------------------

  gl_names <- c(
    "num_trees", "total_num_nodes", "max_depth",
    "impurity", "subsampling_rate"
  )

  # for classification
  gl1 <- glance(rf_classification)

  check_tidy(gl1, exp.row = 1, exp.names = gl_names)

  # for regression
  gl2 <- glance(rf_regression)

  check_tidy(gl2,
    exp.row = 1,
    exp.names = gl_names
  )

  rf_classification_parsnip <- parsnip::rand_forest(engine = "spark") %>%
    parsnip::set_mode("classification") %>%
    parsnip::fit(Species ~ Sepal_Length + Petal_Length, iris_tbl)

  rf_regression_parsnip <- parsnip::rand_forest(engine = "spark") %>%
    parsnip::set_mode("regression") %>%
    parsnip::fit(Sepal_Length ~ Petal_Length + Petal_Width, iris_tbl)

  expect_equal(
    tidy(rf_classification_parsnip)$importance,
    td1$importance,
    tolerance = 0.1, scale = 1
  )

  expect_equal(
    tidy(rf_regression_parsnip)$importance,
    td2$importance,
    tolerance = 0.2, scale = 1
  )

  expect_equal(
    colnames(collect(augment(rf_classification_parsnip))),
    colnames(au1)
  )

  expect_equal(
    collect(augment(rf_regression_parsnip, top_25))$.prediction,
    au2$.prediction,
    tolerance = 0.1, scale = 1
  )

  expect_equal(
    names(glance(rf_classification_parsnip)),
    names(gl1)
  )

  expect_equal(
    names(glance(rf_regression_parsnip)),
    names(gl2)
  )
})

test_clear_cache()

