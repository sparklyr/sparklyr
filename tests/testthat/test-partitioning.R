context("partitioning")

test_that("sdf_repartition works", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(iris_tbl %>%
                 sdf_repartition(4L) %>%
                 sdf_num_partitions(),
               4L)

  expect_equal(iris_tbl %>%
                 sdf_repartition(columns = c("Species", "Petal_Width")) %>%
                 sdf_query_plan() %>%
                 dplyr::first() %>%
                 gsub("#[0-9]+", "", ., perl = TRUE),
               "RepartitionByExpression [Species, Petal_Width]"
                 )

  expect_equal(iris_tbl %>%
                 sdf_repartition(5L, columns = c("Species", "Petal_Width")) %>%
                 sdf_query_plan() %>%
                 dplyr::first() %>%
                 gsub("#[0-9]+", "", ., perl = TRUE),
               "RepartitionByExpression [Species, Petal_Width], 5")

  expect_equal(iris_tbl %>%
                 sdf_repartition(7L, columns = "Species") %>%
                 sdf_num_partitions(),
               7L)
})
