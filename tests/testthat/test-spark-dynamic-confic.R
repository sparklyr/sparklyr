context("spark dynamic configuration")

sc <- testthat_spark_connection()

test_that("configuration getter works", {
  spark_set_config(sc, "spark.sql.shuffle.partitions.local", 1L)
  expect_equal("1",
               spark_get_config(sc, "spark.sql.shuffle.partitions.local"))
  
  # make sure we didn't just get lucky
  spark_set_config(sc, "spark.sql.shuffle.partitions.local", 2L)
  expect_equal("2",
               spark_get_config(sc, "spark.sql.shuffle.partitions.local"))
})