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

  range <- seq(-4, 4, 8e-6)

  weighted_table <- tibble::tibble(
    v = range,
    w = sapply(range, dnorm)
  )[sample(length(range)), ]

  sdf <- copy_to(sc, weighted_table, overwrite = TRUE)

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

test_that("Can generate i.i.d samples from distributions correctly", {
  sample_sz <- 5e5
  seed <- 142857L
  test_cases <- list(
    list(fn = "rbeta", args = list(shape1 = 2.1, shape2 = 4.9)),
    list(fn = "rbinom", args = list(size = 1000, prob = 0.25)),
    list(fn = "rcauchy"),
    list(fn = "rcauchy", args = list(location = 1.5, scale = 1.25)),
    list(fn = "rchisq", args = list(df = 3.4)),
    list(fn = "rexp", args = list(rate = 2.5)),
    list(fn = "rgamma", args = list(shape = 1.5, rate = 0.8)),
    list(fn = "rgeom", args = list(p = 0.2)),
    list(fn = "rhyper", args = list(m = 20, n = 80, k = 40)),
    list(fn = "rlnorm", args = list(meanlog = 0.1, sdlog = 1.1)),
    list(fn = "rnorm"),
    list(fn = "rnorm", args = list(mean = 2.5, sd = 0.8)),
    list(fn = "rpois", args = list(lambda = 2.5)),
    list(fn = "rt", args = list(df = 5.3)),
    list(fn = "rweibull", args = list(shape = 1.5)),
    list(fn = "rweibull", args = list(shape = 1.5, scale = 1.1)),
    list(fn = "runif"),
    list(fn = "runif", args = list(min = -1, max = 1))
  )

  set.seed(seed)
  for (t in test_cases) {
    fn <- t$fn

    args <- as.list(t$args)

    sdf_fn <- getFromNamespace(paste0("sdf_", fn), "sparklyr")

    stats_fn <- getFromNamespace(fn, "stats")

    stats_fn_args <- list(sample_sz) %>% append(args)

    sdf_fn_args <- list(sc, sample_sz) %>%
      append(args) %>%
      append(
        list(seed = seed, output_col = "x")
      )

    spark_probs <- do.call(sdf_fn, sdf_fn_args) %>%
      mutate(nt = ntile(x, n = 10)) %>%
      filter(nt > 1, nt < 10) %>%
      group_by(nt) %>%
      summarise(m = mean(x, na.rm = TRUE)) %>%
      pull()

    r_probs <- tibble(x = do.call(stats_fn, stats_fn_args))  %>%
      mutate(nt = ntile(x, n = 10)) %>%
      filter(nt > 1, nt < 10) %>%
      group_by(nt) %>%
      summarise(m = mean(x, na.rm = TRUE)) %>%
      pull()

    expect_equal(length(spark_probs), 8)

    expect_equal(
      sort(spark_probs),
      sort(r_probs),
      tolerance = 0.1,
      scale = 1,
      info = fn
    )
  }
})
