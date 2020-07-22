context("livy-proxy")

num_open_fds <- function(port) {
  n <- system2(
    "bash",
    args = c("-c", paste0("'lsof -t -i:", as.integer(port), " | wc -l'")),
    stdout = TRUE
  )

  as.integer(n)
}

test_that("Livy connection works with HTTP proxy", {
  sc <- testthat_livy_connection()
  proxy_port <- 9999
  handle <- local_tcp_proxy(proxy_port = proxy_port, dest_port = 8998)
  expect_equal(num_open_fds(proxy_port), 1)

  config <- livy_config(proxy = httr::use_proxy("localhost", proxy_port))
  version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())
  sc <- spark_connect(
    "http://localhost",
    method = "livy",
    config = config,
    version = version
  )
  expect_equal(num_open_fds(proxy_port), 3)
})
