skip_connection("sdf-partition-sizes")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("sdf_partition_sizes works as expected", {
  num_rows <- 100L
  num_partitions <- 10L

  sdf <- sdf_len(sc, num_rows, repartition = num_partitions)
  rs <- sdf_partition_sizes(sdf)

  expect_equal(length(rs$partition_size), num_partitions)
  expect_equal(sum(rs$partition_size), num_rows)
})

test_clear_cache()

