context("broom-multilayer_perceptron")

test_that("multilayer_perceptron.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for multiclass classification
  td1 <- iris_tbl %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4,3,2,3)) %>%
    tidy()

  check_tidy(td1, exp.row = 3, exp.col = 2,
             exp.names = c("layers", "weight_matrix"))
  expect_equal(td1$weight_matrix[[3]],
               matrix(c(2.763929, -55.78433,  52.43706,
                        15.121471,  21.68149, -34.84883,
                        10.408809,   7.81120, -17.52432), nrow = 3, byrow = TRUE),
               tolerance = 0.001)

})

test_that("multilayer_perceptron.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  # with newdata
  au1 <- iris_tbl %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4,3,2,3)) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))
})

test_that("multilayer_perceptron.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4,3,2,3)) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("input_units",
                           "hidden_1_units", "hidden_2_units",
                           "output_units"))
})
