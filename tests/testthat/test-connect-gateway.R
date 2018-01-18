context("connections - gateway")
sc <- testthat_spark_connection()

test_that("gateway connection fails with invalid session", {
  skip_on_cran()

  expect_error(
    spark_connect(master = "sparklyr://localhost:8880/0")
  )
})
