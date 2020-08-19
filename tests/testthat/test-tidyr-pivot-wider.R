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

test_that("implicit missings turn into explicit missings", {
  sdf <- copy_to(sc, tibble::tibble(a = 1:2, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect() %>%
     dplyr::arrange(a)

  expect_equivalent(pv, tibble::tibble(a = 1:2, x = c(1, NA), y = c(NA, 2)))
})

test_that("error when overwriting existing column", {
  sdf <- copy_to(sc, tibble::tibble(a = 1, key = c("a", "b"), val = 1:2))

  expect_error(
    tidyr::pivot_wider(sdf, names_from = key, values_from = val),
    class = "tibble_error_column_names_must_be_unique"
  )
})

test_that("grouping is preserved", {
  sdf <- copy_to(sc, tibble::tibble(g = 1, k = "x", v = 2))
  out <- sdf %>%
    dplyr::group_by(g) %>%
    tidyr::pivot_wider(names_from = k, values_from = v)

  expect_equal(dplyr::group_vars(out), "g")
})
