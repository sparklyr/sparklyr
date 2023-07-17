skip_on_livy()
skip_on_arrow_devel()
skip_unless_synapse_connect()

sc <- testthat_spark_connection()

call_sparkr <- function(method, ...) {
  get(method, envir = asNamespace("SparkR"))(...)
}

test_that("sparklyr gateway for Synapse should be configured properly", {
  connector <- invoke_static(sc, "org.apache.spark.sparklyr.SparklyrConnector", "getOrCreate")
  expect_false(is.null(connector))
  gateway_url <- invoke_method(sc, FALSE, connector, "getUri")
  expect_true(grepl("sparklyr://[^:]+:\\d{1,5}", gateway_url))
})

test_that("sparklyr spark session should be configured properly", {
  expect_equal(
    call_sparkr("callJMethod", call_sparkr("sparkR.session"), "hashCode"),
    invoke_method(sc, FALSE, sc$state$hive_context, "hashCode")
  )
})
