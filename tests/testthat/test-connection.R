skip_connection("connection")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

call_sparkr <- function(method, ...) {
  get(method, envir = asNamespace("SparkR"))(...)
}

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

test_that("sparklyr gateway for Synapse should be configured properly", {
  skip_unless_synapse_connect()
  connector <- invoke_static(
    sc,
    "org.apache.spark.sparklyr.SparklyrConnector",
    "getOrCreate"
  )
  expect_false(is.null(connector))
  gateway_url <- invoke_method(sc, FALSE, connector, "getUri")
  expect_true(grepl("sparklyr://[^:]+:\\d{1,5}", gateway_url))
})

test_that("sparklyr spark session should be configured properly", {
  skip_unless_synapse_connect()
  expect_equal(
    call_sparkr("callJMethod", call_sparkr("sparkR.session"), "hashCode"),
    invoke_method(sc, FALSE, sc$state$hive_context, "hashCode")
  )
})

test_clear_cache()
