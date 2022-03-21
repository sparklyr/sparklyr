skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("configuration getter works", {
  if (spark_version(sc) >= "2.0.0") {
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 1L)
    expect_equal(
      "1",
      unname(unlist(spark_session_config(sc, "spark.sql.shuffle.partitions.local")))
    )

    # make sure we didn't just get lucky
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 2L)
    expect_equal(
      "2",
      unname(unlist(spark_session_config(sc, "spark.sql.shuffle.partitions.local")))
    )
  }
})

test_that("spark_adaptive_query_execution() works", {
  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_adaptive_query_execution(sc, TRUE)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_adaptive_query_execution(sc) %>% unname() %>% unlist(), "true"
  )
})

test_that("spark_coalesce_shuffle_partitions() works", {

  test_requires_version("3.0.0")

  spark_session_config(
    sc, "spark.sql.adaptive.coalescePartitions.enabled", FALSE
  )
  spark_coalesce_shuffle_partitions(sc, TRUE)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc, "spark.sql.adaptive.coalescePartitions.enabled"
    ) %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_coalesce_shuffle_partitions(sc) %>% unname() %>% unlist(), "true"
  )
})

test_that("spark_advisory_shuffle_partition_size() works", {
  test_requires_version("3.0.0")

  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled", FALSE)

  advisory_shuffle_partition_size <- 64 * 1024 * 1024
  spark_advisory_shuffle_partition_size(sc, advisory_shuffle_partition_size)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.advisoryPartitionSizeInBytes") %>%
      unname() %>%
      unlist(),
    as.character(advisory_shuffle_partition_size)
  )
  expect_equal(
    spark_advisory_shuffle_partition_size(sc) %>% unname() %>% unlist(),
    as.character(advisory_shuffle_partition_size)
  )
})

test_that("spark_coalesce_initial_num_partitions() works", {
  test_requires_version("3.0.0")

  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled", FALSE)

  num_partitions <- 64
  spark_coalesce_initial_num_partitions(sc, num_partitions)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc, "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
    ) %>%
      unname() %>%
      unlist(),
    as.character(num_partitions)
  )
  expect_equal(
    spark_coalesce_initial_num_partitions(sc) %>% unname() %>% unlist(),
    as.character(num_partitions)
  )
})

test_that("spark_auto_broadcast_join_threshold() works", {
  spark_session_config(sc, "spark.sql.autoBroadcastJoinThreshold", -1)
  threshold <- 10 * 1024 * 1024
  spark_auto_broadcast_join_threshold(sc, threshold)

  expect_equal(
    spark_session_config(sc, "spark.sql.autoBroadcastJoinThreshold") %>%
      unname() %>%
      unlist(),
    as.character(threshold)
  )
})
