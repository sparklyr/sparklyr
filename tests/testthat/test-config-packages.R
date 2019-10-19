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

test_that("spark_config_packages() does not change config", {
  expect_equal(
    spark_config(),
    spark_config_packages(spark_config(), NULL, "2.4.0")
  )
})

test_that("spark_config_packages() defaults to latest veresion", {
  expect_equal(
    3L,
    strsplit(spark_config_packages(list(), "kafka", "2.3")$sparklyr.shell.packages, ":")[[1]][3] %>%
      strsplit("\\.") %>% `[[`(1) %>% length()
  )
})
