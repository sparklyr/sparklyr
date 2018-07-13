context("livy")

test_that("'livy_config()' works with extended parameters", {
  config <- livy_config(num_executors = 1)

  expect_equal(as.integer(config$livy.numExecutors), 1)
})

test_that("'livy_config()' works with authentication", {
  config_basic <- livy_config(username = "foo", password = "bar")

  expect_equal(
    config_basic$sparklyr.livy.auth,
    httr::authenticate("foo", "bar", type = "basic")
  )

  config_negotiate <- livy_config(negotiate = TRUE)

  expect_equal(
    config_negotiate$sparklyr.livy.auth,
    httr::authenticate("", "", type = "gssnegotiate")
  )
})
