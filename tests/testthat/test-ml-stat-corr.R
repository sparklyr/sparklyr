context("ml stat - correlation")

mtcars_tbl <- testthat_tbl("mtcars")

test_that("ml_corr() works", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  cor_mat <- mtcars_tbl %>%
    ml_corr()
  expect_equal(
    cor_mat %>%
      as.matrix() %>%
      diag(),
    rep(1, 11)
  )
})

test_that("ml_corr() works with assembled column", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  expect_equal(mtcars_tbl %>%
                 ft_vector_assembler(colnames(mtcars_tbl), "features") %>%
                 ml_corr("features") %>%
                 as.matrix() %>%
                 diag(),
               rep(1, 11)
  )
})

test_that("ml_corr() errors on bad column specification", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  expect_error(
    ml_corr(mtcars_tbl, c("foo", "bar")),
    "All columns specified must be in x\\. Failed to find foo, bar."
  )
})
