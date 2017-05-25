context("checkpoint")

sc <- testthat_spark_connection()

test_that("checkpoint directory getting/setting works", {
  expect_equal(spark_get_checkpoint_dir(sc), "")
  spark_set_checkpoint_dir(sc, "foobar")
  expect_match(spark_get_checkpoint_dir(sc), "foobar")
})
