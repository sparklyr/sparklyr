context("spark connections")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'spark_connect' can create a secondary connection", {
  sc2 <- spark_connect(master = "local", app_name = "other")
  spark_disconnect(sc2)

  succeed()
})

test_that("'spark_connect' can provide a 'spark_log'", {
  log <- capture.output({
    spark_log(sc)
  })

  expect_gte(length(log), 1)
})

test_that("'spark_connect' fails with bad configuration'", {
  config <- spark_config()

  config$sparklyr.shell.args <- c("--badargument")
  config$sparklyr.gateway.start.timeout <- 3

  expect_error({
    spark_connect(master = "local", app_name = "bad_connection", config = config)
  })
})
