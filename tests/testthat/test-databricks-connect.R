context("databricks-connect")

test_that("spark connection method is databricks-connect", {
  sc <- testthat_spark_connection(method = "databricks-connect")
  expect_equal(sc$method, "databricks-connect")
})

test_that("csv_file is disabled when using databricks-connect", {
  sc <- testthat_spark_connection(method = "databricks-connect")
  tryCatch(
    spark_data_copy(sc, NULL, "some_df", 1, serializer = "csv_file"),
    error = function(e) {
      expect_equal(e$message, "Using a local file to copy data is not supported for remote clusters")
    }
  )
})
