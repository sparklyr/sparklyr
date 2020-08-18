context("tidyr-pivot-wider")

sc <- testthat_spark_connection()

test_that("can pivot all cols to wide", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, tibble::tibble(key = c("x", "y", "z"), val = 1:3))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(x = 1, y = 2, z = 3))
})

test_that("non-pivoted cols are preserved", {
  sdf <- copy_to(sc, tibble::tibble(a = 1, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(a = 1, x = 1, y = 2))
})
