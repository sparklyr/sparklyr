context("databricks-connect")

skip_unless_databricks_connect()

test_that("spark connection method is configured correctly", {
  spark_home <- Sys.getenv("SPARK_HOME")
  sc <- testthat_spark_connection()

  # test that the connection method is set correctly
  expect_equal(sc$method, "databricks-connect")
  # test that the SPARK_HOME is the value specified in the environment variable, and is not overriden elsewhere
  expect_equal(sc$spark_home, spark_home)
})

test_that("csv_file is disabled when using databricks-connect", {
  sc <- testthat_spark_connection()
  tryCatch(
    sdf_import(NULL, sc, "some_df", 1, serializer = "csv_file"),
    error = function(e) {
      expect_equal(e$message, "Using a local file to copy data is not supported for remote clusters")
    }
  )
})

test_that("spark local property is set", {
  sc <- testthat_spark_connection()

  client_type <- spark_context(sc) %>% invoke("getLocalProperty", "spark.databricks.service.client.type")
  expect_equal(client_type, "sparklyr")
})
