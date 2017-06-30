context("partitioning")

test_that("sdf_repartition works", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(iris_tbl %>%
                 sdf_repartition(4L) %>%
                 sdf_num_partitions(),
               4L)

  expect_equal(iris_tbl %>%
                 sdf_repartition(partition_by = c("Species", "Petal_Width")) %>%
                 sdf_query_plan() %>%
                 dplyr::first() %>%
                 gsub("#[0-9]+", "", ., perl = TRUE),
               "RepartitionByExpression [Species, Petal_Width]"
                 )

  expect_equal(iris_tbl %>%
                 sdf_repartition(5L, partition_by = c("Species", "Petal_Width")) %>%
                 sdf_query_plan() %>%
                 dplyr::first() %>%
                 gsub("#[0-9]+", "", ., perl = TRUE),
               "RepartitionByExpression [Species, Petal_Width], 5")
})

test_that("'sdf_partition' -- 'partitions' argument should take numeric (#735)", {
  iris_tbl <- testthat_tbl("iris")
  expect_equal(iris_tbl %>%
                 sdf_repartition(6, partition_by = "Species") %>%
                 sdf_num_partitions(),
               6)
})

test_that("'sdf_coalesce' works as expected", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(iris_tbl %>%
                 sdf_repartition(5) %>%
                 sdf_coalesce(1) %>%
                 sdf_num_partitions(),
               1)
})
