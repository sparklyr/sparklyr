context("broom-gradient_boosted_trees")

skip_databricks_connect()
test_that("gradient_boosted_trees.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------
  library(parsnip)
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  get_importance <- function(tbl, feature_name) {
    row <- tbl %>%
      dplyr::filter(feature == feature_name) %>%
      select(importance)
    row[["importance"]]
  }

  iris_two <- iris_tbl %>%
    dplyr::filter(Species != "setosa")

  bt_classification <- iris_two  %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Length + Petal_Length, seed = 123)

  bt_regression <- iris_tbl %>%
    ml_gradient_boosted_trees(Sepal_Length ~ Petal_Length + Petal_Width, seed = 123)

  bt_classification_parsnip <- boost_tree(engine = "spark") %>%
    set_mode("classification") %>%
    fit(Species ~ Sepal_Length + Petal_Length, iris_two)

  bt_regression_parsnip <- boost_tree(engine = "spark") %>%
    set_mode("regression") %>%
    fit(Sepal_Length ~ Petal_Length + Petal_Width, iris_tbl)

  ## ----------------------------- tidy() --------------------------------------

  # for classification
  td1 <- tidy(bt_classification)

  check_tidy(td1,
    exp.row = 2,
    exp.names = c("feature", "importance")
  )
  if (spark_version(sc) < "3.0.0") {
    expect_equal(get_importance(td1, "Petal_Length"), 0.594, tolerance = 0.05, scale = 1)
    expect_equal(get_importance(td1, "Sepal_Length"), 0.406, tolerance = 0.05, scale = 1)
  } else {
    expect_equal(get_importance(td1, "Petal_Length"), 0.819, tolerance = 0.05, scale = 1)
    expect_equal(get_importance(td1, "Sepal_Length"), 0.181, tolerance = 0.05, scale = 1)
  }

  # parsnip test
  expect_true(
    all(tidy(bt_classification_parsnip) == td1)
  )

  # for regression
  td2 <- tidy(bt_regression)

  check_tidy(td2,
    exp.row = 2,
    exp.names = c("feature", "importance")
  )
  if (spark_version(sc) < "3.0.0") {
    expect_equal(get_importance(tbl = td2, "Petal_Length"), 0.607, tolerance = 0.001, scale = 1)
    expect_equal(get_importance(tbl = td2, "Petal_Width"), 0.393, tolerance = 0.001, scale = 1)
  } else {
    expect_equal(get_importance(tbl = td2, "Petal_Length"), 0.798, tolerance = 0.001, scale = 1)
    expect_equal(get_importance(tbl = td2, "Petal_Width"), 0.202, tolerance = 0.001, scale = 1)
  }

  # parsnip test
  expect_true(
    all(tidy(bt_regression_parsnip) == td2)
  )

  ## --------------------------- augment() -------------------------------------

  iris_vars <- dplyr::tbl_vars(iris_tbl)

  # for classification without newdata
  au1 <- bt_classification %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 100,  exp.name = c(iris_vars, ".predicted_label"))

  # parsnip test
  expect_true(
   all(collect(augment(bt_classification_parsnip)) == au1)
  )

  # for regression with newdata
  au2 <- bt_regression %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25, exp.name = c(iris_vars, ".prediction"))

  # parsnip test
  ap <- augment(bt_regression_parsnip, new_data = head(iris_tbl, 25))

  expect_true(
   all(collect(ap) == au2)
  )

  ## ---------------------------- glance() -------------------------------------

  gl_names <- c("num_trees", "total_num_nodes", "max_depth", "impurity",
                "step_size", "loss_type", "subsampling_rate")

  # for classification
  gl1 <- glance(bt_classification)
  check_tidy(gl1, exp.row = 1, exp.names = gl_names)
  # parsnip test
  expect_true(
    all(glance(bt_classification_parsnip) == gl1)
  )

  # for regression
  gl2 <- glance(bt_regression)
  check_tidy(gl2, exp.row = 1, exp.names = gl_names)
  # parsnip test
  expect_true(
    all(glance(bt_regression_parsnip) == gl2)
  )
})
