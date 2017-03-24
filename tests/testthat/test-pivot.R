context("pivot")

test_requires("dplyr", "ggplot2")

sc <- testthat_spark_connection()
diamonds_tbl <- testthat_tbl("diamonds")

test_that("we can construct a simple pivot table", {

  s <- diamonds_tbl %>%
    sdf_pivot(cut ~ color) %>%
    arrange(cut) %>%
    collect() %>%
    as.list()

  r <- diamonds %>%
    mutate(cut = as.character(cut), color = as.character(color)) %>%
    reshape2::dcast(cut ~ color) %>%
    arrange(cut) %>%
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
      "avg(depth)"
    )

    gdf %>% invoke("agg", expr, list())
  }

  s <- diamonds_tbl %>%
    sdf_pivot(cut ~ color, fun.aggregate = fun.aggregate) %>%
    arrange(cut) %>%
    collect() %>%
    as.list()

  r <- diamonds %>%
    mutate(cut = as.character(cut), color = as.character(color)) %>%
    reshape2::dcast(cut ~ color, fun.aggregate = mean, value.var = "depth") %>%
    arrange(cut) %>%
    as.list()

  expect_equal(unname(s), unname(r))

})

test_that("we can pivot with an R list", {
  test_requires("dplyr")

  s <- diamonds_tbl %>%
    sdf_pivot(cut ~ color, list(depth = "avg")) %>%
    arrange(cut) %>%
    collect() %>%
    as.list()

  r <- diamonds %>%
    mutate(cut = as.character(cut), color = as.character(color)) %>%
    reshape2::dcast(cut ~ color, mean, value.var = "depth") %>%
    arrange(cut) %>%
    as.list()

  expect_equal(unname(s), unname(r))

})
