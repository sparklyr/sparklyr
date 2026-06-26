skip_connection("tidiers_ml_other")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("als tidiers works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  movies <- data.frame(
    user = c(1, 2, 0, 1, 2, 0),
    item = c(1, 1, 1, 2, 2, 0),
    rating = c(3, 1, 2, 4, 5, 4)
  )

  movies_tbl <- sdf_copy_to(sc, movies, name = "movies_tbl", overwrite = TRUE)

  ## ----------------------------- tidy() --------------------------------------

  als_model <- movies_tbl %>%
    ml_als(rating ~ user + item)

  td1 <- tidy(als_model) %>%
    collect()

  check_tidy(
    td1,
    exp.row = 3,
    exp.col = 3,
    exp.names = c("id", "user_factors", "item_factors")
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- als_model %>%
    augment() %>%
    collect()

  check_tidy(
    au1,
    exp.col = 4,
    exp.name = c("user", "item", "rating", ".prediction")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(als_model)

  check_tidy(gl1, exp.row = 1, exp.names = c("rank", "cold_start_strategy"))
})

test_that("bisecting_kmeans.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  bk_model <- mtcars_tbl %>%
    ml_bisecting_kmeans(~ mpg + cyl, k = 4L, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(bk_model)

  check_tidy(td1, exp.row = 4, exp.names = c("mpg", "cyl", "size", "cluster"))

  expect_equal(td1$size, c(11, 9, 7, 5))

  ## --------------------------- augment() -------------------------------------

  au1 <- bk_model %>%
    augment() %>%
    dplyr::collect()

  check_tidy(
    au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), ".cluster")
  )

  au2 <- bk_model %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25, exp.name = c(names(mtcars), ".cluster"))

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(bk_model)

  if (spark_version(sc) >= "2.3.0") {
    check_tidy(gl1, exp.row = 1, exp.names = c("k", "wssse", "silhouette"))
  } else {
    check_tidy(gl1, exp.row = 1, exp.names = c("k", "wssse"))
  }
})

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
  au1 <- collect(augment(dt_classification))

  check_tidy(
    au1,
    exp.row = nrow(iris),
    exp.name = c(iris_vars, ".predicted_label")
  )

  # for regression with newdata

  top_25 <- iris_tbl %>%
    head(25)

  au2 <- collect(augment(dt_regression, top_25))

  check_tidy(au2, exp.row = 25, exp.name = c(iris_vars, ".prediction"))

  ## ---------------------------- glance() -------------------------------------

  gl_names <- c("num_nodes", "depth", "impurity")

  # for classification
  gl1 <- glance(dt_classification)

  check_tidy(gl1, exp.row = 1, exp.names = gl_names)

  # for regression
  gl2 <- glance(dt_regression)

  check_tidy(gl2, exp.row = 1, exp.names = gl_names)

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


test_that("gaussian_mixture.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  model <- mtcars_tbl %>%
    ml_gaussian_mixture(~ mpg + cyl, k = 4L, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  expect_warning_on_arrow(
    td1 <- tidy(model)
  )

  check_tidy(
    td1,
    exp.row = 4,
    exp.names = c("mpg", "cyl", "weight", "size", "cluster")
  )

  expect_equal(td1$size, model$summary$cluster_sizes())

  ## --------------------------- augment() -------------------------------------

  au1 <- model %>%
    augment() %>%
    collect()

  check_tidy(
    au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), ".cluster")
  )

  au2 <- model %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    collect()

  check_tidy(au2, exp.row = 25, exp.name = c(names(mtcars), ".cluster"))

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(model)

  if (spark_version(sc) >= "2.3.0") {
    check_tidy(gl1, exp.row = 1, exp.names = c("k", "silhouette"))
  } else {
    check_tidy(gl1, exp.row = 1, exp.names = "k")
  }
})

test_that("gradient_boosted_trees.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

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
    dplyr::filter(Species == "setosa")

  bt_classification <- iris_two %>%
    ml_gradient_boosted_trees(Species ~ Sepal_Length + Petal_Length, seed = 123)

  bt_regression <- iris_tbl %>%
    ml_gradient_boosted_trees(
      Sepal_Length ~ Petal_Length + Petal_Width,
      seed = 123
    )

  ## ----------------------------- tidy() --------------------------------------

  # for classification
  td1 <- tidy(bt_classification)

  check_tidy(td1, exp.row = 2, exp.names = c("feature", "importance"))

  if (spark_version(sc) < "3.0.0") {
    expect_equal(
      get_importance(td1, "Petal_Length"),
      0.594,
      tolerance = 0.05,
      scale = 1
    )
    expect_equal(
      get_importance(td1, "Sepal_Length"),
      0.406,
      tolerance = 0.05,
      scale = 1
    )
  } else {
    expect_equal(
      get_importance(td1, "Petal_Length"),
      0.819,
      tolerance = 0.05,
      scale = 1
    )
    expect_equal(
      get_importance(td1, "Sepal_Length"),
      0.181,
      tolerance = 0.05,
      scale = 1
    )
  }

  # for regression
  td2 <- tidy(bt_regression)

  check_tidy(td2, exp.row = 2, exp.names = c("feature", "importance"))
  if (spark_version(sc) < "3.0.0") {
    expect_equal(
      get_importance(tbl = td2, "Petal_Length"),
      0.607,
      tolerance = 0.001,
      scale = 1
    )
    expect_equal(
      get_importance(tbl = td2, "Petal_Width"),
      0.393,
      tolerance = 0.001,
      scale = 1
    )
  } else {
    expect_equal(
      get_importance(tbl = td2, "Petal_Length"),
      0.798,
      tolerance = 0.001,
      scale = 1
    )
    expect_equal(
      get_importance(tbl = td2, "Petal_Width"),
      0.202,
      tolerance = 0.001,
      scale = 1
    )
  }

  ## --------------------------- augment() -------------------------------------

  iris_vars <- dplyr::tbl_vars(iris_tbl)

  # for classification without newdata
  au1 <- bt_classification %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 50, exp.name = c(iris_vars, ".predicted_label"))

  # for regression with newdata
  au2 <- bt_regression %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25, exp.name = c(iris_vars, ".prediction"))

  ## ---------------------------- glance() -------------------------------------

  gl_names <- c(
    "num_trees",
    "total_num_nodes",
    "max_depth",
    "impurity",
    "step_size",
    "loss_type",
    "subsampling_rate"
  )

  # for classification
  gl1 <- glance(bt_classification)
  check_tidy(gl1, exp.row = 1, exp.names = gl_names)

  # for regression
  gl2 <- glance(bt_regression)

  check_tidy(gl2, exp.row = 1, exp.names = gl_names)

  bt_classification_parsnip <- parsnip::boost_tree(engine = "spark") %>%
    parsnip::set_mode("classification") %>%
    parsnip::fit(Species ~ Sepal_Length + Petal_Length, iris_two)

  bt_regression_parsnip <- parsnip::boost_tree(engine = "spark") %>%
    parsnip::set_mode("regression") %>%
    parsnip::fit(Sepal_Length ~ Petal_Length + Petal_Width, iris_tbl)

  expect_true(all(tidy(bt_classification_parsnip) == td1))

  expect_true(all(tidy(bt_regression_parsnip) == td2))

  expect_true(
    all(collect(augment(bt_classification_parsnip)) == au1)
  )

  ap <- augment(bt_regression_parsnip, new_data = head(iris_tbl, 25))

  expect_true(all(collect(ap) == au2))

  expect_true(all(glance(bt_classification_parsnip) == gl1))

  expect_true(all(glance(bt_regression_parsnip) == gl2))
})

test_that("kmeans.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  model <- mtcars_tbl %>%
    ml_kmeans(~ mpg + cyl, k = 4L, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(model)

  check_tidy(td1, exp.row = 4, exp.names = c("mpg", "cyl", "size", "cluster"))
  expect_equal(td1$size, model$summary$cluster_sizes())

  ## --------------------------- augment() -------------------------------------

  au1 <- model %>%
    augment() %>%
    collect()

  check_tidy(
    au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), ".cluster")
  )

  ## ---------------------------- glance() -------------------------------------

  au2 <- model %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    collect()

  check_tidy(au2, exp.row = 25, exp.name = c(names(mtcars), ".cluster"))

  gl1 <- glance(model)

  if (spark_version(sc) >= "2.3.0") {
    check_tidy(gl1, exp.row = 1, exp.names = c("k", "wssse", "silhouette"))
  } else {
    check_tidy(gl1, exp.row = 1, exp.names = c("k", "wssse"))
  }
})

test_that("lda.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  samples <- data.frame(
    text = c(
      "The cat sat on the mat.",
      "The dog ate my homework."
    )
  )

  lines_tbl <- sdf_copy_to(sc, samples, name = "lines_tbl", overwrite = TRUE)

  lda_model <- lines_tbl %>%
    ml_lda(~text, k = 3)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(lda_model)

  check_tidy(
    td1,
    exp.row = 18,
    exp.col = 3,
    exp.names = c("topic", "term", "beta")
  )

  # account for LDA method behavior change in Spark 3.0.0
  expect_equal(
    td1$beta[1:3],
    ifelse(
      spark_version(sc) < "3.0.0",
      list(c(0.8773, 0.9466, 1.2075)),
      list(c(0.8790, 0.9478, 1.1515))
    )[[1]],
    tolerance = 0.001,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  expect_warning_on_arrow(
    au1 <- lda_model %>%
      augment() %>%
      collect()
  )

  check_tidy(au1, exp.col = 2, exp.name = c("text", ".topic"))

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(lda_model)

  check_tidy(
    gl1,
    exp.row = 1,
    exp.names = c(
      "k",
      "vocab_size",
      "learning_decay",
      "optimizer"
    )
  )
})

test_that("pca tidiers work", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  pca_model <- iris_tbl %>%
    select(-Species) %>%
    ml_pca(k = 3)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(pca_model)

  model <- iris %>%
    dplyr::select(-Species) %>%
    stats::prcomp()

  check_tidy(
    td1,
    exp.row = 4,
    exp.col = 4,
    exp.names = c("features", "PC1", "PC2", "PC3")
  )

  ## --------------------------- augment() -------------------------------------

  expect_equal(
    td1$PC1,
    -as.vector(model$rotation[, 1]),
    tolerance = 0.001,
    scale = 1
  )

  au1 <- pca_model %>%
    augment(head(iris_tbl, 25)) %>%
    collect()

  check_tidy(
    au1,
    exp.row = 25,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      "PC1",
      "PC2",
      "PC3"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(pca_model)

  check_tidy(
    gl1,
    exp.row = 1,
    exp.names = c(
      "k",
      "explained_variance_PC1",
      "explained_variance_PC2",
      "explained_variance_PC3"
    )
  )
})

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

  check_tidy(
    au1,
    exp.row = nrow(iris),
    exp.name = c(iris_vars, ".predicted_label")
  )

  top_25 <- iris_tbl %>%
    head(25)

  au2 <- collect(augment(rf_regression, top_25))

  check_tidy(au2, exp.row = 25, exp.name = c(iris_vars, ".prediction"))

  ## ---------------------------- glance() -------------------------------------

  gl_names <- c(
    "num_trees",
    "total_num_nodes",
    "max_depth",
    "impurity",
    "subsampling_rate"
  )

  # for classification
  gl1 <- glance(rf_classification)

  check_tidy(gl1, exp.row = 1, exp.names = gl_names)

  # for regression
  gl2 <- glance(rf_regression)

  check_tidy(gl2, exp.row = 1, exp.names = gl_names)

  rf_classification_parsnip <- parsnip::rand_forest(engine = "spark") %>%
    parsnip::set_mode("classification") %>%
    parsnip::fit(Species ~ Sepal_Length + Petal_Length, iris_tbl)

  rf_regression_parsnip <- parsnip::rand_forest(engine = "spark") %>%
    parsnip::set_mode("regression") %>%
    parsnip::fit(Sepal_Length ~ Petal_Length + Petal_Width, iris_tbl)

  expect_equal(
    tidy(rf_classification_parsnip)$importance,
    td1$importance,
    tolerance = 0.1,
    scale = 1
  )

  expect_equal(
    tidy(rf_regression_parsnip)$importance,
    td2$importance,
    tolerance = 0.2,
    scale = 1
  )

  expect_equal(
    colnames(collect(augment(rf_classification_parsnip))),
    colnames(au1)
  )

  expect_equal(
    collect(augment(rf_regression_parsnip, top_25))$.prediction,
    au2$.prediction,
    tolerance = 0.1,
    scale = 1
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
