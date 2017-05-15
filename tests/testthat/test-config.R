context("config")

test_that("spark_config_exists function works as expected", {
  expect_false(
    spark_config_exists(list(), "sparklyr.log.console", FALSE)
  )

  expect_true(
    spark_config_exists(list(), "sparklyr.log.console", TRUE)
  )

  expect_true(
    spark_config_exists(list("sparklyr.log.console"), "sparklyr.log.console", FALSE)
  )

  expect_true(
    spark_config_exists(list(sparklyr.log.console = TRUE), "sparklyr.log.console", FALSE)
  )

  expect_true(
    spark_config_exists(list(sparklyr.log.console = FALSE), "sparklyr.log.console", TRUE)
  )
})

test_that("spark_config_value function works as expected", {
  expect_equals(
    "1",
    spark_config_value(list(), "sparklyr.nothing", "1")
  )

  expect_equals(
    "2",
    spark_config_value(list(sparklyr.something = 2), "sparklyr.something", "1")
  )

  expect_equals(
    1,
    spark_config_value(list(), "sparklyr.nothing", 1)
  )

  expect_equals(
    2,
    spark_config_value(list(sparklyr.something = 2), "sparklyr.something", 1)
  )
})
