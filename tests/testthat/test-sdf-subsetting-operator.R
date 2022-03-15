
sc <- testthat_spark_connection()

df <- tibble::tibble(
  col1 = rep(1L, 5L),
  col2 = rep("two", 5L),
  col3 = rep(3.33333, 5L),
  col4 = seq(5L),
  col5 = c(NA_real_, 1.1, 2.2, 3.3, 4.4),
  col6 = c("a", NA, "b", "c", NA)
)
sdf <- copy_to(sc, df, overwrite = TRUE)

expect_subsets_eq <- function(x) {
  expect_equivalent(df[x], sdf[x] %>% collect())
}

test_that("`[.tbl_spark` works as expected", {
  test_cases <- list(
    4L, 2:4, 4:2, -3L, -2:-4, c(2L, 4L), "col5", c("col1", "col3", "col6"), NULL
  )

  for (x in test_cases) {
    expect_subsets_eq(x)
  }

  expect_equivalent(df[], sdf[] %>% collect())
})
