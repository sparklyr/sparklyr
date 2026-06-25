skip_connection("tidiers_ml_classification")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("logistic_regression tidiers work", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  iris_tbl <- testthat_tbl("iris") %>%
    filter(Species != "setosa")

  iris_local <- iris %>%
    filter(Species != "setosa")

  # Fitting model using sparklyr api
  lr_model <- iris_tbl %>%
    ml_logistic_regression(
      Species ~ Sepal_Length + Sepal_Width,
      family = "binomial"
    )

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(lr_model)

  check_tidy(
    td1,
    exp.row = 3,
    exp.col = 2,
    exp.names = c("features", "coefficients")
  )

  expect_equal(
    td1$coefficients,
    c(-13.0460, 1.9024, 0.4047),
    tolerance = 0.01,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  # without newdata
  au1 <- collect(augment(lr_model))

  check_tidy(
    au1,
    exp.row = nrow(iris_local),
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  # with newdata
  au2 <- collect(augment(lr_model, newdata = iris_tbl))

  check_tidy(
    au2,
    exp.row = nrow(iris_local),
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  ## ---------------------------- glance() -------------------------------------
  gu1 <- glance(lr_model)

  check_tidy(gu1, exp.row = 1, exp.names = c("elastic_net_param", "lambda"))

  lr_parsnip <- parsnip::logistic_reg(engine = "spark") %>%
    parsnip::fit(Species ~ Sepal_Length + Sepal_Width, iris_tbl)

  expect_true(all(tidy(lr_parsnip) == td1))

  expect_true(all(collect(augment(lr_parsnip)) == au1))

  expect_true(all(collect(augment(lr_parsnip, new_data = iris_tbl)) == au2))

  expect_error(augment(lr_parsnip, newdata = iris_tbl))

  expect_true(all(glance(lr_parsnip) == gu1))
})

test_that("naive_bayes.tidy() works", {
  library(dplyr)
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  nb_model <- iris_tbl %>%
    ml_naive_bayes(Species ~ Sepal_Length + Petal_Length)

  ## ----------------------------- tidy() --------------------------------------

  # for multiclass classification
  td1 <- tidy(nb_model)

  check_tidy(
    td1,
    exp.row = 3,
    exp.col = 4,
    exp.names = c(".label", "Sepal_Length", "Petal_Length", ".pi")
  )

  expect_equal(
    sort(td1$Sepal_Length),
    sort(c(-0.258, -0.542, -0.612)),
    tolerance = 0.001,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  # with newdata
  au1 <- nb_model %>%
    augment(head(iris_tbl, 25)) %>%
    collect()

  check_tidy(
    au1,
    exp.row = 25,
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(nb_model)

  check_tidy(gl1, exp.row = 1, exp.names = c("model_type", "smoothing"))
})

test_that("broom interface for Linear SVC works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  svc_model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_linear_svc(Species ~ ., reg_param = 0.01, max_iter = 10)

  td1 <- tidy(svc_model)

  ## ----------------------------- tidy() --------------------------------------

  expected_coefs <- c(-0.06004978, -0.1563083, -0.460648, 0.2276626, 1.055085)
  if (spark_version(sc) >= "3.2.0") {
    expected_coefs <- c(
      -6.8823988,
      -0.6154984,
      -1.5135447,
      1.9694126,
      3.3736856
    )
  }

  check_tidy(
    td1,
    exp.row = 5,
    exp.col = 2,
    exp.names = c("features", "coefficients")
  )
  expect_equal(td1$coefficients, expected_coefs, tolerance = 0.01, scale = 1)

  ## --------------------------- augment() -------------------------------------

  au1 <- svc_model %>%
    augment() %>%
    collect()

  check_tidy(
    au1,
    exp.row = 100,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".predicted_label"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(svc_model)

  check_tidy(
    gl1,
    exp.row = 1,
    exp.names = c("reg_param", "standardization", "aggregation_depth")
  )
})

test_that("multilayer_perceptron.tidy() works", {
  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")
  partitions <- iris_tbl %>%
    sdf_random_split(train = 0.75, test = 0.25, seed = 1099)

  mp_model <- partitions$train %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4, 6, 3, 3))

  # for multiclass classification
  acc <- ml_predict(mp_model, partitions$test) %>%
    ml_multiclass_classification_evaluator(metric_name = "accuracy")

  expect_gt(acc, 0.94)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(mp_model)

  check_tidy(
    td1,
    exp.row = 3,
    exp.col = 2,
    exp.names = c("layers", "weight_matrix")
  )

  expected_coeffs <- ifelse(
    spark_version(sc) < "3.0.0",
    list(c(
      285.3834,
      -268.631159,
      -18.461112,
      -41.7810,
      8.394612,
      35.739773,
      -284.7548,
      284.738913,
      -1.015223,
      135.0024,
      -137.314854,
      2.800369
    )),
    list(c(
      -377.28496,
      70.13146,
      306.6542,
      -73.48908,
      140.72911,
      -68.5861,
      344.95784,
      -140.89405,
      -205.1463,
      73.88816,
      52.27940,
      -125.8437
    ))
  )[[1]]

  expect_equal(
    td1$weight_matrix[[3]],
    matrix(expected_coeffs, nrow = 4, byrow = TRUE),
    tolerance = 0.001,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- mp_model %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(
    au1,
    exp.row = 25,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".predicted_label"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(mp_model)

  check_tidy(
    gl1,
    exp.row = 1,
    exp.names = c(
      "input_units",
      "hidden_1_units",
      "hidden_2_units",
      "output_units"
    )
  )
})

test_clear_cache()
