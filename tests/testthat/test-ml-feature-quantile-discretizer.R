context("ml feature quantile discretizer")

test_that("ft_quantile_discretizer works", {
  df <- data_frame(
    id = 0:4L,
    hour = c(18, 19, 8, 5, 2)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  result <- df_tbl %>%
    ft_quantile_discretizer("hour", "result", num_buckets = 3) %>%
    pull(result)

  result2 <- df_tbl %>%
    ft_quantile_discretizer(., "hour", "result", num_buckets = 3, dataset = .) %>%
    pull(result)

  expect_identical(result, c(2, 2, 1, 1, 0))
  expect_identical(result2, c(2, 2, 1, 1, 0))
})

test_that("ft_quantile_discretizer works on multiple columns", {
  test_requires_version("2.3.0", "multiple columns support requires spark 2.3+")
  df <- data_frame(
    id = 0:4L,
    hour = c(18, 19, 8, 5, 2),
    hour2 = c(5, 2, 12, 6, 1)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  result <- df_tbl %>%
    ft_quantile_discretizer(
      input_cols = c("hour", "hour2"),
      output_cols = c("result", "result2"),
      num_buckets_array = c(3, 2)) %>%
    collect()

  expect_identical(names(result), c("id", "hour", "hour2", "result", "result2"))
})
