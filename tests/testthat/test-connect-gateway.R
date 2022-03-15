sc <- testthat_spark_connection()

test_that("gateway connection fails with invalid session", {
  expect_error(
    spark_connect(master = "sparklyr://localhost:8880/0")
  )
})

test_that("can connect to an existing session via gateway", {
  gw <- spark_connect(
    master = paste0("sparklyr://localhost:8880/", sc$sessionId)
  )
  expect_equal(spark_context(gw)$backend, spark_context(sc)$backend)
})
