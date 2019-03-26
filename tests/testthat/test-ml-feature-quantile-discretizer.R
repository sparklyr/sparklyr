context("ml feature quantile discretizer")

test_that("ft_quantile_discretizer() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_quantile_discretizer)
})

test_that("ft_quantile_discretizer() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    num_buckets = 3,
    handle_invalid = "keep",
    relative_error = 0.002
  )
  test_param_setting(sc, ft_quantile_discretizer, test_args)

  test_args2 <- list(
    input_cols = c("foo1", "foo2"),
    output_cols = c("bar1", "bar2"),
    num_buckets_array = c(3, 4),
    handle_invalid = "keep",
    relative_error = 0.002
  )
  test_param_setting(sc, ft_quantile_discretizer, test_args2)
})

test_that("ft_quantile_discretizer works", {
  sc <- testthat_spark_connection()
  df <- data_frame(
    id = 0:4L,
    hour = c(18, 19, 8, 5, 2)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  result <- df_tbl %>%
    ft_quantile_discretizer("hour", "result", num_buckets = 3) %>%
    pull(result)

  expect_identical(result, c(2, 2, 1, 1, 0))
})

test_that("ft_quantile_discretizer works on multiple columns", {
  test_requires_version("2.3.0", "multiple columns support requires spark 2.3+")
  sc <- testthat_spark_connection()
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
