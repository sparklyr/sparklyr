skip_connection("connection_databricks")
skip_on_livy()
skip_on_arrow_devel()

# ---- Connection-free tests --------------------------------------------------
# These exercise the parts of the Databricks/Synapse connectors that do not
# require a live Databricks notebook, Synapse session, or SparkR. They must sit
# *above* skip_unless_databricks_connect(), which otherwise aborts the file.

test_that("databricks_expand_jars copies missing compatibility jars", {
  # the sparklyr-2.2/2.1 compatibility jars are not shipped, so the copy branch
  # runs for both; only file.copy is mocked (mocking file.exists globally breaks
  # package/namespace lookup, which relies on it)
  copied <- character(0)
  with_mocked_bindings(
    spark_default_app_jar = function(version, ...) {
      paste0("/jars/app-", version, ".jar")
    },
    .package = "sparklyr",
    with_mocked_bindings(
      file.copy = function(from, to, ...) {
        copied <<- c(copied, to)
        TRUE
      },
      .package = "base",
      databricks_expand_jars()
    )
  )
  # one copy for each missing version (2.2, 2.1)
  expect_length(copied, 2)
})

test_that("databricks_connection errors when the SparkR backend is unavailable", {
  with_mocked_bindings(
    databricks_expand_jars = function() invisible(NULL),
    .package = "sparklyr",
    expect_error(
      databricks_connection(config = list(), extensions = NULL),
      "Failed to start sparklyr backend"
    )
  )
})

test_that("synapse_connection errors when the SparkR backend is unavailable", {
  home <- withr::local_tempdir()
  with_mocked_bindings(
    spark_default_app_jar = function(...) "/jars/x.jar",
    .package = "sparklyr",
    expect_error(
      synapse_connection(
        spark_home = home,
        spark_version = "3.5.0",
        scala_version = "2.12",
        config = list(),
        extensions = NULL
      ),
      "\\[Synapse\\] Failed to start sparklyr backend"
    )
  )
})

test_that("spark_version methods return the cached version", {
  sc_db <- structure(
    list(state = list(spark_version = numeric_version("3.5.0"))),
    class = c("databricks_connection", "spark_connection")
  )
  expect_equal(
    spark_version.databricks_connection(sc_db),
    numeric_version("3.5.0")
  )

  sc_syn <- structure(
    list(state = list(spark_version = numeric_version("3.4.1"))),
    class = c("synapse_connection", "spark_connection")
  )
  expect_equal(
    spark_version.synapse_connection(sc_syn),
    numeric_version("3.4.1")
  )
})

skip_unless_databricks_connect()

test_that("spark connection method is configured correctly", {
  spark_home <- Sys.getenv("SPARK_HOME")
  sc <- testthat_spark_connection()

  # test that the connection method is set correctly
  expect_equal(sc$method, "databricks-connect")
  # test that the SPARK_HOME is the value specified in the environment variable, and is not overriden elsewhere
  expect_equal(sc$spark_home, spark_home)
})

test_that("spark local property is set", {
  sc <- testthat_spark_connection()

  client_type <- spark_context(sc) %>%
    invoke("getLocalProperty", "spark.databricks.service.client.type")
  expect_equal(client_type, "sparklyr")
})

test_that("spark libpaths config is set", {
  sc <- testthat_spark_connection()

  expect_false(is.null(sc$config$spark.r.libpaths))
})

test_clear_cache()
