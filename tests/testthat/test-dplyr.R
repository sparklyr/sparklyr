context("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("the implementation of 'mutate' functions as expected", {
  test_requires("dplyr")

  expect_equal(
    iris %>% mutate(x = Species) %>% tbl_vars() %>% length(),
    iris_tbl %>% mutate(x = Species) %>% collect() %>% tbl_vars() %>% length()
  )
})
