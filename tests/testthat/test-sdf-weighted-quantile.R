context("sdf-weighted-quantile")

set.seed(31415926L)
sc <- testthat_spark_connection()

test_that("sdf_weighted_quantile() works as expected", {
  range <- seq(-4, 4, 1e-5)

  sdf <- copy_to(
    sc,
    tibble::tibble(
      v = range,
      w = sapply(range, dnorm)
    )[sample(length(range)), ],
    repartition = 100L
  )

  pct <- seq(0, 1, 0.001)

  for (max_error in c(0.1, 0.05, 0.01, 0.001)) {
    pct_values <- sdf_weighted_quantile(
      sdf,
      column = "v",
      weightColumn = "w",
      probabilities = pct,
      max.error = max_error
    )
    approx_pct <- purrr::map_dbl(pct_values, pnorm)

    expect_equal(length(approx_pct), length(pct))
    for (i in seq_along(pct)) {
      expect_equal(pct[[i]], approx_pct[[i]], tol = max_error, scale = 1)
    }
  }
})
