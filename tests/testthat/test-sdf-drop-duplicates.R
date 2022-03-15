
test_requires("dplyr")

sc <- testthat_spark_connection()

test_that("sdf_describe() works properly", {
  iris_tbl <- testthat_tbl("iris")

  s <- iris_tbl %>%
    select(Petal_Length, Species) %>%
    sdf_drop_duplicates()

  expect_equal(sdf_nrow(s), 48)

  s <- iris_tbl %>% sdf_drop_duplicates(c("Species"))
  expect_equal(sdf_nrow(s), 3)
})

test_that("sdf_drop_duplicates() checks column name", {
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    sdf_drop_duplicates(iris_tbl, c("Sepal_Length", "foo")),
    "The following columns are not in the data frame: foo"
  )
})
