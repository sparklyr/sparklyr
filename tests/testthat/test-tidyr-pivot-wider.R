context("tidyr-pivot-wider")

sc <- testthat_spark_connection()

test_that("can pivot all cols to wide", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(key = c("x", "y", "z"), val = 1:3))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(x = 1, y = 2, z = 3))
})

test_that("non-pivoted cols are preserved", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = 1, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(a = 1, x = 1, y = 2))
})

test_that("implicit missings turn into explicit missings", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = 1:2, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect() %>%
     dplyr::arrange(a)

  expect_equivalent(pv, tibble::tibble(a = 1:2, x = c(1, NaN), y = c(NaN, 2)))
})

test_that("error when overwriting existing column", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = 1, key = c("a", "b"), val = 1:2))

  expect_error(
    tidyr::pivot_wider(sdf, names_from = key, values_from = val),
    class = "tibble_error_column_names_must_be_unique"
  )
})

test_that("grouping is preserved", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(g = 1, k = "x", v = 2))
  out <- sdf %>%
    dplyr::group_by(g) %>%
    tidyr::pivot_wider(names_from = k, values_from = v)

  expect_equal(dplyr::group_vars(out), "g")
})

test_that("nested list column pivots correctly", {
  test_requires_version("2.4.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(
      i = c(1, 2, 1, 2),
      g = c("a", "a", "b", "b"),
      d = list(
        list(x = 1, y = 5), list(x = 2, y = 6), list(x = 3, y = 7), list(x = 4, y = 8)
      )
    )
  )
  out <- tidyr::pivot_wider(sdf, names_from = g, values_from = d, names_sort = TRUE) %>%
    collect() %>%
    dplyr::arrange(i)

  expect_equivalent(
    out,
    tibble::tibble(
      i = 1:2,
      a = list(list(x = 1, y = 5), list(x = 2, y = 6)),
      b = list(list(x = 3, y = 7), list(x = 4, y = 8))
    )
  )
})

test_that("can specify output column names using names_glue", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(x = c("X", "Y"), y = 1:2, a = 1:2, b = 1:2)
  )

  expect_equivalent(
    tidyr::pivot_wider(
      sdf,
      names_from = x:y,
      values_from = a:b,
      names_glue = "{x}{y}_{.value}",
      names_sort = TRUE
    ) %>%
      collect(),
    tibble::tibble(X1_a = 1, Y2_a = 2, X1_b = 1, Y2_b = 2)
  )
})

test_that("can sort column names", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(int = c(1, 3, 2), days = c("Mon", "Tues", "Wed"))
  )

  expect_equivalent(
    tidyr::pivot_wider(
      sdf, names_from = days, values_from = int, names_sort = TRUE
    ) %>%
      collect(),
    tibble::tibble(Mon = 1, Tues = 3, Wed = 2)
  )
})

test_that("can override default keys", {
  test_requires_version("2.3.0")
  skip_databricks_connect()

  sdf <- copy_to(
    sc,
    tibble::tribble(
      ~row, ~name, ~var,     ~value,
      1,    "Sam", "age",    10,
      2,    "Sam", "height", 1.5,
      3,    "Bob", "age",    20,
    )
  )

  expect_equivalent(
    sdf %>%
      tidyr::pivot_wider(id_cols = name, names_from = var, values_from = value) %>%
      collect(),
    tibble::tribble(
      ~name, ~age, ~height,
      "Sam", 10,   1.5,
      "Bob", 20,   NaN,
    )
  )
})

test_that("values_fn can be a single function", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c(1, 10, 100))
  )
  pv <- tidyr::pivot_wider(
    sdf, names_from = key, values_from = val, values_fn = sum
  ) %>%
    collect() %>%
    dplyr::arrange(a)

  expect_equivalent(pv, tibble::tibble(a = 1:2, x = c(11, 100)))
})

test_that("values_summarize applied even when no-duplicates", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = c(1, 2), key = c("x", "x"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf,
    names_from = key,
    values_from = val,
    values_fn = list(val = rlang::expr(collect_list))
  ) %>%
    collect() %>%
    dplyr::arrange(a)

  expect_equal(pv$a, c(1, 2))
  expect_equivalent(pv, tibble::tibble(a = 1:2, x = list(1, 2)))
})

test_that("can fill in missing cells", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(g = 1:2, var = c("x", "y"), val = 1:2))

  widen <- function(...) {
    sdf %>%
      tidyr::pivot_wider(names_from = var, values_from = val, ...) %>%
      collect() %>%
      dplyr::arrange(g)
  }

  expect_equivalent(
    widen(), tibble::tibble(g = 1:2, x = c(1, NaN), y = c(NaN, 2))
  )
  expect_equivalent(
    widen(values_fill = 0), tibble::tibble(g = 1:2, x = c(1, 0), y = c(0, 2))
  )
  expect_equivalent(
    widen(values_fill = list(val = 0)),
    tibble::tibble(g = 1:2, x = c(1, 0), y = c(0, 2))
  )
})

test_that("values_fill only affects missing cells", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(g = c(1, 2), names = c("x", "y"), value = c(1, NA))
  )
  out <- sdf %>%
    tidyr::pivot_wider(names_from = names, values_from = value, values_fill = 0 ) %>%
    collect() %>%
    dplyr::arrange(g)

  expect_equivalent(out, tibble::tibble(g = 1:2, x = c(1, 0), y = c(0, NaN)))
})

test_that("can pivot from multiple measure cols", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc, tibble::tibble(row = 1, var = c("x", "y"), a = 1:2, b = 3:4)
  )
  pv <- tidyr::pivot_wider(sdf, names_from = var, values_from = c(a, b)) %>%
    collect()

  expect_equivalent(
    pv,
    tibble::tibble(row = 1, a_x = 1, a_y = 2, b_x = 3, b_y = 4)
  )
})

test_that("can pivot from multiple measure cols using all keys", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(var = c("x", "y"), a = 1:2, b = 3:4))
  pv <- tidyr::pivot_wider(sdf, names_from = var, values_from = c(a, b)) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(a_x = 1, a_y = 2, b_x = 3, b_y = 4))
})
