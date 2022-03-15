skip_on_livy()
skip_on_arrow_devel()

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

test_that("'livy_config()' works with additional_curl_opts", {
  config <- livy_config()
  expect_null(config$sparklyr.livy.curl_opts)

  curl_opts <- list(accepttimeout_ms = 5000, verbose = 1)
  config <- livy_config(curl_opts = curl_opts)
  expect_equal(config$sparklyr.livy.curl_opts, curl_opts)
})
