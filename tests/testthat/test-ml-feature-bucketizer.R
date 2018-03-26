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
