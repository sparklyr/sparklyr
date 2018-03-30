context("ml feature bucketizer")

sc <- testthat_spark_connection()

test_that("ft_bucketizer() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(mtcars_tbl %>%
                     dplyr::select(drat) %>%
                     ft_bucketizer("drat", "drat_out",
                                   splits = c(-Inf, 2, 4, Inf)
                     ) %>%
                     colnames(),
                   c("drat", "drat_out")
  )
})

test_that("ft_bucketizer() can mutate multiple columns", {
  test_requires_version("2.3.0", "multiple column support requires 2.3+")
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(mtcars_tbl %>%
    dplyr::select(drat, hp) %>%
    ft_bucketizer(input_cols = c("drat", "hp"),
                  output_cols = c("drat_out", "hp_out"),
                  splits_array = list(c(-Inf, 2, 4, Inf), c(-Inf, 90, 120, Inf))
    ) %>%
    colnames(),
    c("drat", "hp", "drat_out", "hp_out")
  )
})

test_that("ft_quantile_discretizer works", {
  test_requires("dplyr")
  df <- data_frame(id = 0:4L,
                   hour = c(18, 19, 8, 5, 2))
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

test_that("ft_quantile_descretizer() param setting", {
  args <- list(
    x = sc,
    input_col = "hour",
    output_col = "result",
    num_buckets = 3L) %>%
    param_add_version("2.1.0", handle_invalid = "skip") %>%
    param_add_version("2.0.0", relative_error = 0.01)

  qd <- do.call(ft_quantile_discretizer, args)

  expect_equal(
    ml_params(qd, names(args)[-1]),
    args[-1])
})

test_that("ft_quantile_discretizer works on multiple columns", {
  test_requires_version("2.3.0", "multiple columns support requires spark 2.3+")
  test_requires("dplyr")
  df <- data_frame(id = 0:4L,
                   hour = c(18, 19, 8, 5, 2),
                   hour2 = c(5, 2, 12, 6, 1))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  result <- df_tbl %>%
    ft_quantile_discretizer(
      input_cols = c("hour", "hour2"),
      output_cols = c("result", "result2"),
      num_buckets_array = c(3, 2)) %>%
    collect()

  expect_identical(names(result), c("id", "hour", "hour2", "result", "result2"))
})
