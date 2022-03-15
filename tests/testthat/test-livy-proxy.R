skip_on_livy()
skip_on_arrow_devel()

num_open_fds <- function(port) {
  n <- system2(
    "bash",
    args = c("-c", paste0("'lsof -t -i:", as.integer(port), " | wc -l'")),
    stdout = TRUE
  )

  as.integer(n)
}

test_that("Livy connection works with HTTP proxy", {
  sc <- testthat_spark_connection()
  if (!identical(sc$method, "livy")) {
    skip("Test only applicable to Livy connections")
  }

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
  expect_gte(num_open_fds(proxy_port), 2)

  expect_equivalent(sdf_len(sc, 10) %>% collect(), tibble::tibble(id = seq(10)))

  spark_disconnect(sc)
})
