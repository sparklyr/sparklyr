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
    spark_config_exists(
      list("sparklyr.log.console"),
      "sparklyr.log.console",
      FALSE
    )
  )

  expect_true(
    spark_config_exists(
      list(sparklyr.log.console = TRUE),
      "sparklyr.log.console",
      FALSE
    )
  )

  expect_false(
    spark_config_exists(
      list(sparklyr.log.console = FALSE),
      "sparklyr.log.console",
      TRUE
    )
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
        spark_config_value(
          list(sparklyr.something = 2),
          "sparklyr.something",
          "1"
        )
      )

      expect_equal(
        1,
        spark_config_value(list(), "sparklyr.nothing", 1)
      )

      expect_equal(
        2,
        spark_config_value(
          list(sparklyr.something = 2),
          "sparklyr.something",
          1
        )
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
        "--conf",
        "key1=value1",
        "--conf",
        "key2=value2",
        "--key3",
        "value3",
        "--packages",
        "pkg1,pkg2,pkg3"
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

test_that("spark_config_packages() supports kafka", {
  expect_equal(
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0",
    spark_config_packages(list(), "kafka", "2.4.0")$sparklyr.shell.packages
  )
})

test_that("spark_config_packages() supports versioned packages", {
  expect_equal(
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0",
    spark_config_packages(
      list(),
      "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0",
      "2.4.0"
    )$sparklyr.shell.packages
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
    strsplit(
      spark_config_packages(list(), "kafka", "2.3")$sparklyr.shell.packages,
      ":"
    )[[1]][3] %>%
      strsplit("\\.") %>%
      `[[`(1) %>%
      length()
  )
})

test_that("spark_config_settings() returns a data frame of named settings", {
  settings <- spark_config_settings()
  expect_s3_class(settings, "data.frame")
  expect_named(settings, c("name", "description"))
  expect_true(nrow(settings) > 0)
  expect_true("sparklyr.arrow" %in% settings$name)
  expect_true(all(nchar(settings$description) > 0))
})

test_that("spark_config() picks up SPARK_DRIVER_CLASSPATH", {
  withr::with_envvar(
    list(SPARK_DRIVER_CLASSPATH = "/some/classpath"),
    {
      config <- spark_config()
      expect_equal(
        config$master$`sparklyr.shell.driver-class-path`,
        "/some/classpath"
      )
    }
  )
})

test_that("spark_config() reads SPARKLYR_CONFIG_FILE when set", {
  configFilePath <- tempfile(pattern = "envconfig_", fileext = ".yml")
  writeLines(
    c("default:", "  sparklyr.env.entry: 99"),
    con = configFilePath
  )
  withr::with_envvar(
    list(SPARKLYR_CONFIG_FILE = configFilePath),
    {
      config <- spark_config()
      expect_equal(config$sparklyr.env.entry, 99)
    }
  )
})

test_that("merge_lists() handles empty, recursive, and overwrite cases", {
  # empty base returns overlay
  expect_equal(merge_lists(list(), list(a = 1)), list(a = 1))
  # empty overlay returns base
  expect_equal(merge_lists(list(a = 1), list()), list(a = 1))
  # recursive merge of nested lists
  merged <- merge_lists(
    list(outer = list(a = 1, b = 2)),
    list(outer = list(b = 3, c = 4))
  )
  expect_equal(merged$outer$a, 1)
  expect_equal(merged$outer$b, 3)
  expect_equal(merged$outer$c, 4)
  # non-list overlay overwrites scalar
  merged2 <- merge_lists(list(a = 1, b = 2), list(a = 10))
  expect_equal(merged2$a, 10)
  expect_equal(merged2$b, 2)
})

test_that("spark_config_value() enforces documented settings when option is on", {
  withr::with_options(
    list(sparklyr.test.enforce.config = TRUE),
    {
      # documented setting passes through
      expect_equal(
        spark_config_value(
          list(sparklyr.arrow = TRUE),
          "sparklyr.arrow",
          FALSE
        ),
        TRUE
      )
      # sparklyr.shell.* is exempt from the documented-settings check
      expect_equal(
        spark_config_value(
          list("sparklyr.shell.packages" = "x"),
          "sparklyr.shell.packages"
        ),
        "x"
      )
      # an undocumented sparklyr.* name errors
      expect_error(
        spark_config_value(list(), "sparklyr.not.a.real.setting", FALSE),
        "not described in spark_config_settings"
      )
    }
  )
})

test_that("spark_config_value() falls back to global options", {
  withr::with_options(
    list(
      sparklyr.test.enforce.config = FALSE,
      sparklyr.value.from.option = 42
    ),
    {
      expect_equal(
        spark_config_value(list(), "sparklyr.value.from.option", 0),
        42
      )
    }
  )
})

test_that("spark_config_value() evaluates function and language values", {
  withr::with_options(
    list(sparklyr.test.enforce.config = FALSE),
    {
      # function value gets called
      expect_equal(
        spark_config_value(
          list(sparklyr.fn = function() 7),
          "sparklyr.fn"
        ),
        7
      )
      # a formula lambda (a language object) is converted to a closure
      # via rlang::as_closure() and then called
      expect_equal(
        spark_config_value(
          list(sparklyr.lang = ~ 3 + 4),
          "sparklyr.lang"
        ),
        7
      )
    }
  )
})

test_that("spark_config_integer() and spark_config_logical() coerce values", {
  withr::with_options(
    list(sparklyr.test.enforce.config = FALSE),
    {
      expect_identical(
        spark_config_integer(list(sparklyr.x = "5"), "sparklyr.x"),
        5L
      )
      expect_identical(
        spark_config_logical(list(sparklyr.x = "TRUE"), "sparklyr.x"),
        TRUE
      )
      expect_identical(
        spark_config_logical(list(), "sparklyr.x", FALSE),
        FALSE
      )
    }
  )
})

test_that("spark_config_value_retries() returns value on success", {
  expect_equal(
    spark_config_value_retries(
      list(sparklyr.x = 11),
      "sparklyr.x",
      default = 0,
      retries = 3
    ),
    11
  )
})

test_that("spark_config_value_retries() retries and eventually stops on failure", {
  attempts <- 0
  with_mocked_bindings(
    spark_config_value = function(config, name, default = NULL) {
      # allow the verbose lookup to resolve normally
      if (identical(name, "sparklyr.verbose")) {
        return(FALSE)
      }
      attempts <<- attempts + 1
      stop("boom")
    },
    {
      expect_error(
        spark_config_value_retries(list(), "sparklyr.x", 0, retries = 2),
        "Failed after"
      )
    },
    .package = "sparklyr"
  )
  expect_true(attempts >= 1)
})

test_that("spark_config_value_retries() emits verbose error message", {
  with_mocked_bindings(
    spark_config_value = function(config, name, default = NULL) {
      if (identical(name, "sparklyr.verbose")) {
        return(TRUE)
      }
      stop("kapow")
    },
    {
      expect_message(
        expect_error(
          spark_config_value_retries(list(), "sparklyr.x", 0, retries = 1),
          "Failed after"
        ),
        "failed with error: kapow"
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() builds delta packages across versions", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      # Spark 3.5 uses delta-spark and sets sql extensions
      cfg_35 <- spark_config_packages(list(), "delta", "3.5.0")
      expect_match(
        cfg_35$sparklyr.shell.packages,
        "io.delta:delta-spark_2.12:3.0.0"
      )
      expect_equal(
        cfg_35$`spark.sql.extensions`,
        "io.delta.sql.DeltaSparkSessionExtension"
      )
      expect_equal(
        cfg_35$`spark.sql.catalog.spark_catalog`,
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )

      # Spark 4.0 uses scala 2.13 and delta-spark 4.0.0
      cfg_40 <- spark_config_packages(list(), "delta", "4.0.0")
      expect_match(
        cfg_40$sparklyr.shell.packages,
        "io.delta:delta-spark_2.13:4.0.0"
      )

      # Spark 3.0 uses delta-core (pre 3.5) and scala 2.12
      cfg_30 <- spark_config_packages(list(), "delta", "3.0.0")
      expect_match(
        cfg_30$sparklyr.shell.packages,
        "io.delta:delta-core_2.12:0.8.0"
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() errors on delta below 2.4.2", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      expect_error(
        spark_config_packages(list(), "delta", "2.3.0"),
        "Delta Lake requires Spark 2.4.2 or newer"
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() errors on kafka below 2.0", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      expect_error(
        spark_config_packages(list(), "kafka", "1.6.0"),
        "Kafka requires Spark 2.x"
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() supports avro", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      cfg <- spark_config_packages(list(), "avro", "3.0.0")
      expect_match(
        cfg$sparklyr.shell.packages,
        "org.apache.spark:spark-avro_2.12:3.0.0"
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() supports rapids in both methods", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      cfg <- spark_config_packages(
        list(),
        "rapids",
        "3.0.0",
        method = "local"
      )
      expect_true(
        "com.nvidia:rapids-4-spark_2.12:0.1.0" %in%
          cfg$sparklyr.shell.packages
      )
      expect_true(
        "ai.rapids:cudf:0.14" %in% cfg$sparklyr.shell.packages
      )
      expect_true(
        "spark.plugins=com.nvidia.spark.SQLPlugin" %in%
          cfg$sparklyr.shell.conf
      )

      cfg_db <- spark_config_packages(
        list(),
        "rapids",
        "3.0.0",
        method = "databricks"
      )
      expect_true(
        "com.nvidia:rapids-4-spark_2.12:0.1.0-databricks" %in%
          cfg_db$sparklyr.shell.packages
      )
    },
    .package = "sparklyr"
  )
})

test_that("spark_config_packages() errors on rapids below 3.0", {
  with_mocked_bindings(
    spark_version_latest = function(version = NULL) version,
    {
      expect_error(
        spark_config_packages(list(), "rapids", "2.4.0", method = "local"),
        "RAPIDS library requires Spark 3.0.0 or higher"
      )
    },
    .package = "sparklyr"
  )
})

test_that("worker_config_serialize()/deserialize() round-trip", {
  config <- list(
    debug = TRUE,
    profile = FALSE,
    schema = TRUE,
    arrow = FALSE,
    fetch_result_as_sdf = TRUE,
    single_binary_column = FALSE,
    spark_read = TRUE,
    spark_version = "3.5.0"
  )
  serialized <- worker_config_serialize(config)
  expect_type(serialized, "character")

  deserialized <- worker_config_deserialize(serialized)
  expect_true(deserialized$debug)
  expect_false(deserialized$profile)
  expect_true(deserialized$schema)
  expect_false(deserialized$arrow)
  expect_true(deserialized$fetch_result_as_sdf)
  expect_false(deserialized$single_binary_column)
  expect_true(deserialized$spark_read)
  expect_equal(deserialized$spark_version, "3.5.0")
  # defaults for gateway port / address
  expect_equal(deserialized$sparklyr.gateway.port, 8880L)
  expect_equal(deserialized$sparklyr.gateway.address, "localhost")
})

test_clear_cache()
