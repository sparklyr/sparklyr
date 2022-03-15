
test_that("sdf_repartition works", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(
    iris_tbl %>%
      sdf_repartition(4L) %>%
      sdf_num_partitions(),
    4L
  )
})

test_that("sdf_reparition: partitioning by column works", {
  test_requires_version("2.0.0", "partitioning by column requires spark 2.0+")
  iris_tbl <- testthat_tbl("iris")

  if (is_testing_databricks_connect()) {
    # DB Connect's optimized plans don't display much useful information when calling toString,
    # so we use the analyzed plan instead
    plan_type <- "analyzed"
  } else {
    plan_type <- "optimizedPlan"
  }

  expect_true(
    any(grepl(
      "RepartitionByExpression \\[Species, Petal_Width\\]",
      iris_tbl %>%
        sdf_repartition(partition_by = c("Species", "Petal_Width")) %>%
        sdf_query_plan(plan_type) %>%
        gsub("#[0-9]+", "", ., perl = TRUE)
    ))
  )

  expect_true(
    any(grepl(
      "RepartitionByExpression \\[Species, Petal_Width\\], 5",
      iris_tbl %>%
        sdf_repartition(5L, partition_by = c("Species", "Petal_Width")) %>%
        sdf_query_plan(plan_type) %>%
        gsub("#[0-9]+", "", ., perl = TRUE)
    ))
  )
})

test_that("'sdf_partition' -- 'partitions' argument should take numeric (#735)", {
  test_requires_version("2.0.0", "partitioning by column requires spark 2.0+")
  iris_tbl <- testthat_tbl("iris")
  expect_equal(
    iris_tbl %>%
      sdf_repartition(6, partition_by = "Species") %>%
      sdf_num_partitions(),
    6
  )
})

test_that("'sdf_coalesce' works as expected", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(
    iris_tbl %>%
      sdf_repartition(5) %>%
      sdf_coalesce(1) %>%
      sdf_num_partitions(),
    1
  )
})
