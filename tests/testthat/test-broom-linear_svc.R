context("broom-linear_svc")

test_that("linear_svc.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  # for binary classification
  td1 <- iris_tbl %>%
    dplyr::filter(Species != "setosa") %>%
    ml_linear_svc(Species ~ .) %>%
    tidy()

  check_tidy(td1, exp.row = 5, exp.col = 2,
             exp.names = c("features", "coefficients"))
  expect_equal(td1$coefficients, c(-4.55, -0.981, -2.85, 1.87, 6.06),
               tolerance = 0.001)

})

test_that("linear_svc.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  iris_tbl2 <- iris_tbl %>%
    filter(Species != "setosa")

  # with newdata
  au1 <- iris_tbl2 %>%
    ml_linear_svc(Species ~ .) %>%
    augment(head(iris_tbl2, 25)) %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          ".predicted_label"))
})

test_that("linear_svc.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    dplyr::filter(Species != "setosa") %>%
    ml_linear_svc(Species ~ .) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("reg_param", "standardization", "aggregation_depth"))
})
