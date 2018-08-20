context("broom-kmeans")

test_that("kmeans.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  td1 <- mtcars_tbl %>%
    ml_kmeans(~ mpg + cyl, k = 4L, seed = 123) %>%
    tidy()

  check_tidy(td1, exp.row = 4,
             exp.names = c("mpg", "cyl", "size", "cluster"))
  expect_equal(td1$size, c(14, 2, 6 , 10))

})

test_that("kmeans.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")

  au1 <- mtcars_tbl %>%
    ml_kmeans(~ mpg + cyl, k = 4L, seed = 123) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.row = nrow(mtcars),
             exp.name = c(names(mtcars), ".cluster"))

  au2 <- mtcars_tbl %>%
    ml_kmeans(~ mpg + cyl, k = 4L, seed = 123) %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2, exp.row = 25,
             exp.name = c(names(mtcars), ".cluster"))
})

test_that("kmeans.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")

  version <- spark_version(sc)

  gl1 <- mtcars_tbl %>%
    ml_kmeans(~ mpg + cyl, k = 4L, seed = 123) %>%
    glance()

  if (version >= "2.3.0"){
    check_tidy(gl1, exp.row = 1,
               exp.names = c("k", "wssse", "silhouette"))
  } else {
    check_tidy(gl1, exp.row = 1,
               exp.names = c("k", "wssse"))
  }
})
