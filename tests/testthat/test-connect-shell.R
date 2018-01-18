context("connections - shell")

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

test_that("'spark_session_id' generates different ids for different apps", {
  expect_true(
    spark_session_id(app_name = "foo", master = "local") !=
    spark_session_id(app_name = "bar", master = "local")
  )
})

test_that("'spark_session_id' generates same ids for same apps", {
  expect_equal(
    spark_session_id(app_name = "foo", master = "local"),
    spark_session_id(app_name = "foo", master = "local")
  )
})

test_that("'spark_session_random' generates different ids even with seeds", {
  expect_true({
    set.seed(10)
    spark_session_random()
  } != {
    set.seed(10)
    spark_session_random()
  })
})

test_that("'spark_inspect' can enumerate information from the context", {
  skip_on_cran()

  result <- capture.output({
    sparklyr:::spark_inspect(spark_context(sc))
  })

  expect_gte(length(result), 100)
})
