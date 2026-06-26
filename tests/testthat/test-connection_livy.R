skip_connection("connection_livy")
skip_unless_livy()
skip_on_arrow_devel()


num_open_fds <- function(port) {
  n <- system2(
    "bash",
    args = c("-c", paste0("'lsof -t -i:", as.integer(port), " | wc -l'")),
    stdout = TRUE
  )

  as.integer(n)
}

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

test_that("Livy connection works with HTTP proxy", {
  sc <- testthat_spark_connection()
  proxy_port <- 9999
  np <- num_open_fds(proxy_port)

  if (np > 0) {
    handle <- local_tcp_proxy(proxy_port = proxy_port, dest_port = 8998)

    expect_equal(np, 1)

    config <- livy_config(proxy = httr::use_proxy("localhost", proxy_port))

    version <- testthat_spark_env_version()

    sc <- spark_connect(
      "http://localhost",
      method = "livy",
      config = config,
      version = version
    )
    expect_gte(num_open_fds(proxy_port), 2)

    expect_equivalent(
      sdf_len(sc, 10) %>% collect(),
      dplyr::tibble(id = seq(10))
    )

    spark_disconnect(sc)
  }
})

test_clear_cache()
