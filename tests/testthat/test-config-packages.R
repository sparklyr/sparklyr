context("config-packages")

test_that("spark_config_packages() supports kafka", {
  expect_equal(
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0",
    spark_config_packages(list(), "kafka", "2.4.0")$sparklyr.shell.packages
  )
})

test_that("spark_config_packages() supports versioned packages", {
  expect_equal(
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0",
    spark_config_packages(list(), "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0", "2.4.0")$sparklyr.shell.packages
  )
})
