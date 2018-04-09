context("correlate")

test_requires("dplyr", "ggplot2")

sc <- testthat_spark_connection()
diamonds_tbl <- testthat_tbl("diamonds")

remote_d <- diamonds_tbl %>%
  select(x, y ,z)

local_d <- ggplot2::diamonds %>%
  select(x,y, z)

test_that("Fails when non supported arguments are passed",{
  expect_error(correlate(remote_d))
  expect_error(correlate(remote_d, y = remote_d))
  expect_error(correlate(remote_d, method = "kendall"))
  expect_error(correlate(remote_d, diagonal = 1))
})

test_that("tbl_sql routine returns same results are the default routine",{
  expect_equal(
    correlate(remote_d, use = "complete.obs", quiet = TRUE) %>%
      tidyr::gather() %>%
      arrange(value) %>%
      pull(value),
    correlate(local_d, use = "complete.obs", quiet = TRUE) %>%
      tidyr::gather() %>%
      arrange(value) %>%
      filter(key != "rowname") %>%
      pull(value) %>%
      as.numeric()
  )
})
