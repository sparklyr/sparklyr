skip_connection("config")
skip_on_livy()
skip_on_arrow_devel()

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
  configs <- list(
    list(
      sparklyr.shell.conf = "key1=value1",
      sparklyr.shell.conf = "key2=value2",
      sparklyr.shell.key3 = "value3",
      sparklyr.shell.packages = c("pkg1", "pkg2", "pkg3")
    ),
    list(
      sparklyr.shell.conf = c("key1=value1", "key2=value2"),
      sparklyr.shell.key3 = "value3",
      sparklyr.shell.packages = c("pkg1", "pkg2", "pkg3")
    )
  )
  for (config in configs) {
    expect_equal(
      spark_config_shell_args(config = config, master = "local"),
      c(
        "--conf", "key1=value1",
        "--conf", "key2=value2",
        "--key3", "value3",
        "--packages", "pkg1,pkg2,pkg3"
      )
    )
  }
})

test_that("spark_config() warns if invalid config file that exists is passed", {
  configFilePath <- tempfile(pattern = "config_", fileext = ".yml")
  invalidConfigContent <- c(
    "default:",
    "  spark.num.executors: 1",
    "  spark.executor.memory: 2g",
    "  spark.executor.memory: 5g"
  )
  writeLines(text = invalidConfigContent, con = configFilePath)
  expect_warning(
    spark_config(file = configFilePath),
    "Error reading config file:"
  )
})

test_that("spark_config() is silent if no config file is passed", {
  expect_silent(
    spark_config()
  )
})

test_that("spark_config warns() if non-existent file is passed", {
  expect_warning(
    spark_config(file = tempfile(paste(sample(letters, 5L), collapse = ""))),
    "Error reading config file:"
  )
})
