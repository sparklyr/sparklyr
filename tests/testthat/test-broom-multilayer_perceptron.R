context("broom-multilayer_perceptron")

skip_databricks_connect()
test_that("multilayer_perceptron.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")
  partitions <- iris_tbl %>%
    sdf_random_split(train = 0.75, test = 0.25, seed = 1099)

  # for multiclass classification
  td1 <- partitions$train %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4, 6, 3, 3))

  acc <- ml_predict(td1, partitions$test) %>%
    ml_multiclass_classification_evaluator(metric_name = "accuracy")
  expect_gt(acc, 0.94)

  td1 <- td1 %>% tidy()
  check_tidy(td1,
    exp.row = 3, exp.col = 2,
    exp.names = c("layers", "weight_matrix")
  )

  expected_coeffs <- ifelse(spark_version(sc) < "3.0.0",
    list(c(
      285.3834, -268.631159, -18.461112,
      -41.7810, 8.394612, 35.739773,
      -284.7548, 284.738913, -1.015223,
      135.0024, -137.314854, 2.800369
    )),
    list(c(
      -377.28496, 70.13146, 306.6542,
      -73.48908, 140.72911, -68.5861,
      344.95784, -140.89405, -205.1463,
      73.88816, 52.27940, -125.8437
    ))
  )[[1]]

  expect_equal(td1$weight_matrix[[3]],
    matrix(expected_coeffs, nrow = 4, byrow = TRUE),
    tolerance = 0.001, scale = 1
  )
})

test_that("multilayer_perceptron.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # with newdata
  au1 <- iris_tbl %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4, 3, 2, 3)) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au1,
    exp.row = 25,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      ".predicted_label"
    )
  )
})

test_that("multilayer_perceptron.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4, 3, 2, 3)) %>%
    glance()

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c(
      "input_units",
      "hidden_1_units", "hidden_2_units",
      "output_units"
    )
  )
})
