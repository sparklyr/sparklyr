context("sdf-distinct")

sc <- testthat_spark_connection()

test_that("sdf_distinct works properly", {

  test_requires_version("1.3.0")
  skip_databricks_connect()

  sdf <- sdf_copy_to(sc, datasets::mtcars, overwrite = TRUE)

  expect_equal(
    dim(datasets::mtcars),
    sdf %>% sdf_distinct() %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct(cyl, gear) %>% sdf_dim()
  )

  expect_equal(
    c(3L, 1L),
    sdf %>% sdf_distinct(cyl) %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct(c("cyl", "gear")) %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct("cyl", "gear") %>% sdf_dim()
  )

  expect_equal(
    c(3L, 1L),
    sdf %>% sdf_distinct("cyl") %>% sdf_dim()
  )

  expect_equal(
    data.frame(
      vs = c(0, 0, 0, 1, 1, 1, 1),
      gear = c(3, 4, 5, 3, 4, 4, 5),
      am = c(0, 1, 1, 0, 0, 1, 1)
    ),
    sdf %>% sdf_distinct(vs, c("gear", "am")) %>%
      arrange_all() %>% collect() %>% as.data.frame()
  )

})
