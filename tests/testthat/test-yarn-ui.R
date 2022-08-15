skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
sc <- testthat_spark_connection()

test_that("'spark_connection_yarn_ui()' can build a default URL", {
  expect_true(
    nchar(spark_connection_yarn_ui(sc)) > 0
  )
})
