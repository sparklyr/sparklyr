context("spark apply")
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'spark_apply' can apply identity function", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e) %>% collect(),
    iris_tbl %>% collect()
  )
})

test_that("'spark_apply' can filter columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[1:1]) %>% collect(),
    iris_tbl %>% select(Sepal_Length) %>% collect()
  )
})

test_that("'spark_apply' can add columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) cbind(e, 1), names = c(colnames(iris_tbl), "new")) %>% collect(),
    iris_tbl %>% mutate(new = 1) %>% collect()
  )
})

test_that("'spark_apply' can concatenate", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) apply(e, 1, paste, collapse = " ")) %>% collect(),
    iris_tbl %>% transmute(s = paste(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, Species)) %>% collect()
  )
})

test_that("'spark_apply' can filter", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[e$Species == "setosa",]) %>% collect(),
    iris_tbl %>% filter(Species == "setosa") %>% collect()
  )
})

test_that("'spark_apply' works with 'sdf_repartition'", {
  expect_equal(
    iris_tbl %>% sdf_repartition(2L) %>% spark_apply(function(e) e) %>% collect(),
    iris_tbl %>% collect()
  )
})
