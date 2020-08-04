context("config")

test_that("spark_config_exists function works as expected", {
  expect_false(
    spark_config_exists(list(), "sparklyr.log.console", FALSE)
  )

  expect_true(
    spark_config_exists(list(), "sparklyr.log.console", TRUE)
  )

  expect_false(
    spark_config_exists(list("sparklyr.log.console"), "sparklyr.log.console", FALSE)
  )

  expect_true(
    spark_config_exists(list(sparklyr.log.console = TRUE), "sparklyr.log.console", FALSE)
  )

  expect_false(
    spark_config_exists(list(sparklyr.log.console = FALSE), "sparklyr.log.console", TRUE)
  )
})

test_that("spark_config_value function works as expected", {
  withr::with_options(
    list(sparklyr.test.enforce.config = FALSE),
    {
      expect_equal(
        "1",
        spark_config_value(list(), "sparklyr.nothing", "1")
      )

      expect_equal(
        2,
        spark_config_value(list(sparklyr.something = 2), "sparklyr.something", "1")
      )

      expect_equal(
        1,
        spark_config_value(list(), "sparklyr.nothing", 1)
      )

      expect_equal(
        2,
        spark_config_value(list(sparklyr.something = 2), "sparklyr.something", 1)
      )
    }
  )
})

test_that("spark_config() can load from options", {
  options(sparklyr.test.entry = 10)

  config <- spark_config()
  expect_equal(
    config$sparklyr.test.entry,
    10
  )
})

test_that("spark_config_shell_args() works as expected", {
  config <- list(
    sparklyr.shell.conf = "key1=value1",
    sparklyr.shell.conf = "key2=value2",
    sparklyr.shell.key3 = "value3",
    sparklyr.shell.packages = c("pkg1", "pkg2", "pkg3")
  )

  expect_equal(
    spark_config_shell_args(config = config, master = "local"),
    c("--conf", "key1=value1", "--conf", "key2=value2", "--key3", "value3", "--packages", "pkg1,pkg2,pkg3")
  )
})
