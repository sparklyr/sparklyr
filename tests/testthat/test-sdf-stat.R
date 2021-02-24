context("sdf stat")

sc <- testthat_spark_connection()

test_that("sdf_crosstab() works", {
  mtcars_tbl <- testthat_tbl("mtcars")
  df <- mtcars_tbl %>%
    sdf_crosstab("cyl", "gear") %>%
    collect()
  expect_setequal(names(df), c("cyl_gear", "3.0", "4.0", "5.0"))
  expect_setequal(df[, 1, drop = TRUE], c("8.0", "4.0", "6.0"))
})

test_that("sdf_quantile() works for a single column", {
  test_requires_version("2.0.0", "approxQuantile() is only supported in Spark 2.0.0 or above")

  mtcars_tbl <- testthat_tbl("mtcars")
  quantiles <- mtcars_tbl %>%
    sdf_quantile(column = "disp")
  expect_mapequal(
    quantiles,
    c(
      `0%` = 71.1,
      `25%` = 120.3,
      `50%` = 167.6,
      `75%` = 318,
      `100%` = 472
    )
  )
})

test_that("sdf_quantile() works for multiple column", {
  test_requires_version("2.0.0", "multicolumn quantile requires 2.0+")
  mtcars_tbl <- testthat_tbl("mtcars")

  quantiles <- mtcars_tbl %>%
    sdf_quantile(column = c("disp", "drat"))
  expect_named(quantiles, c("disp", "drat"))
  expect_mapequal(
    quantiles[["disp"]],
    c(
      `0%` = 71.1,
      `25%` = 120.3,
      `50%` = 167.6,
      `75%` = 318,
      `100%` = 472
    )
  )
  expect_mapequal(
    quantiles[["drat"]],
    c(
      `0%` = 2.76,
      `25%` = 3.08,
      `50%` = 3.69,
      `75%` = 3.92,
      `100%` = 4.93
    )
  )
})

test_that("sdf_quantile() approximates weighted quantiles correctly", {
  set.seed(31415926L)
  range <- seq(-4, 4, 5e-6)

  sdf <- copy_to(
    sc,
    tibble::tibble(
      v = range,
      w = sapply(range, dnorm)
    )[sample(length(range)), ],
    repartition = 1000L
  )

  pct <- seq(0, 1, 0.001)

  for (max_error in c(0.1, 0.05, 0.01, 0.001)) {
    pct_values <- sdf_quantile(sdf, "v", pct, max_error, "w")
    approx_pct <- purrr::map_dbl(pct_values, pnorm)

    expect_equal(length(approx_pct), length(pct))
    for (i in seq_along(pct)) {
      expect_equal(pct[[i]], approx_pct[[i]], tol = max_error, scale = 1)
    }
  }
})
