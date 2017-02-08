context("pivot")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")

test_that("we can construct a simple pivot table", {
  test_requires("dplyr")

  s <- mtcars_tbl %>%
    sdf_pivot(cyl ~ am) %>%
    arrange(cyl) %>%
    collect() %>%
    as.list()

  r <- mtcars %>%
    reshape2::dcast(cyl ~ am) %>%
    arrange(cyl) %>%
    as.list()

  expect_equal(unname(s), unname(r))

})

test_that("we can pivot with an R function for aggregation", {
  test_requires("dplyr")

  fun.aggregate <- function(gdf) {

    expr <- invoke_static(
      sc,
      "org.apache.spark.sql.functions",
      "expr",
      "avg(mpg)"
    )

    gdf %>% invoke("agg", expr, list())
  }

  s <- mtcars_tbl %>%
    sdf_pivot(cyl ~ am, fun.aggregate = fun.aggregate) %>%
    arrange(cyl) %>%
    collect() %>%
    as.list()

  r <- mtcars %>%
    reshape2::dcast(cyl ~ am, fun.aggregate = mean, value.var = "mpg") %>%
    arrange(cyl) %>%
    as.list()

  expect_equal(unname(s), unname(r))

})

test_that("we can pivot with an R list", {
  test_requires("dplyr")

  s <- mtcars_tbl %>%
    sdf_pivot(cyl ~ am, list(mpg = "avg")) %>%
    arrange(cyl) %>%
    collect() %>%
    as.list()

  r <- mtcars %>%
    reshape2::dcast(cyl ~ am, mean, value.var = "mpg") %>%
    arrange(cyl) %>%
    as.list()

  expect_equal(unname(s), unname(r))

})
