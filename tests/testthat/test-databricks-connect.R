skip_connection("databricks-connect")
skip_on_livy()
skip_on_arrow_devel()

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

  client_type <- spark_context(sc) %>% invoke("getLocalProperty", "spark.databricks.service.client.type")
  expect_equal(client_type, "sparklyr")
})

test_that("spark libpaths config is set", {
  sc <- testthat_spark_connection()

  expect_false(is.null(sc$config$spark.r.libpaths))
})

test_clear_cache()

