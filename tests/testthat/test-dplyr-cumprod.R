skip_connection("dplyr-cumprod")
skip_on_livy()
#TODO: databricks - Remove after figuring what to do about cumprod
skip()
sc <- testthat_spark_connection()

test_that("cumprod works as expected", {
  test_requires_version("2.0.0")
  test_requires("dplyr")

  for (stats in list(
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, 0, 10)),
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, NA, 0, 10)),
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, NA, 10))
  )) {
    stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

    expected <- stats %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      )

    actual <- stats_tbl %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      ) %>%
      collect() %>%
      as.data.frame()

    expect_equal(actual, expected)
  }
})
