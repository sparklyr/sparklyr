context("ml feature bucketizer")

test_that("ft_bucketizer() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_bucketizer)
})

test_that("ft_bucketizer() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "x",
    output_col = "y",
    splits = c(-1, 34, 100),
    handle_invalid = "keep"
  )
  test_param_setting(sc, ft_bucketizer, test_args)

  test_args2 <- list(
    input_cols = c("x1", "x2"),
    output_cols = c("y1", "y2"),
    splits_array = list(c(-1, 34, 100), c(-5, 0, 2)),
    handle_invalid = "keep"
  )
  test_param_setting(sc, ft_bucketizer, test_args2)
})

test_that("ft_bucketizer() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(
    mtcars_tbl %>%
      select(drat) %>%
      ft_bucketizer(
        "drat", "drat_out",
        splits = c(-Inf, 2, 4, Inf)
      ) %>%
      colnames(),
    c("drat", "drat_out")
  )
})

test_that("ft_bucketizer() can mutate multiple columns", {
  test_requires_version("2.3.0", "multiple column support requires 2.3+")
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(
    mtcars_tbl %>%
      select(drat, hp) %>%
      ft_bucketizer(
        input_cols = c("drat", "hp"),
        output_cols = c("drat_out", "hp_out"),
        splits_array = list(c(-Inf, 2, 4, Inf), c(-Inf, 90, 120, Inf))
      ) %>%
      colnames(),
    c("drat", "hp", "drat_out", "hp_out")
  )
})
