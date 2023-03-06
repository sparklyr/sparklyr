skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("can pivot all cols to wide", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(key = c("x", "y", "z"), val = 1:3))
  pv <- tidyr::pivot_wider(
    sdf,
    names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(x = 1, y = 2, z = 3))
})

test_that("non-pivoted cols are preserved", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = 1, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf,
    names_from = key, values_from = val, names_sort = TRUE
  ) %>%
    collect()

  expect_equivalent(pv, tibble::tibble(a = 1, x = 1, y = 2))
})

test_that("implicit missings turn into explicit missings", {
  test_requires_version("2.3.0")

  sdf <- copy_to(sc, tibble::tibble(a = 1:2, key = c("x", "y"), val = 1:2))
  pv <- tidyr::pivot_wider(
    sdf,
    names_from = key, values_from = val, names_sort = TRUE
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

  df <- tibble::tibble(g = 1, k = "x", v = 2)

  sdf <- copy_to(sc, df, overwrite = TRUE)

  sdf_out <- sdf %>%
    group_by(g) %>%
    pivot_wider(names_from = k, values_from = v)

  expect_equal(dplyr::group_vars(sdf_out), "g")
})

test_that("nested list column pivots correctly", {
  test_requires_version("2.4.0")

  df <- tibble::tibble(
    i = c(1, 2, 1, 2),
    g = c("a", "a", "b", "b"),
    d = list(
      list(x = 1, y = 5), list(x = 2, y = 6), list(x = 3, y = 7), list(x = 4, y = 8)
    ))

  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    sdf_out <- pivot_wider(sdf, names_from = g, values_from = d, names_sort = TRUE) %>%
      collect() %>%
      arrange(i)
  )

  df_out <- pivot_wider(df, names_from = g, values_from = d, names_sort = TRUE) %>%
    arrange(i)

  expect_equal(sdf_out, df_out)
})

test_that("can specify output column names using names_glue", {
  test_requires_version("2.3.0")

  df <- tibble::tibble(x = c("X", "Y"), y = 1:2, a = 1:2, b = 1:2)

  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sparklyr::pivot_wider(
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
      sdf,
      names_from = days, values_from = int, names_sort = TRUE
    ) %>%
      collect(),
    tibble::tibble(Mon = 1, Tues = 3, Wed = 2)
  )
})

test_that("can override default keys", {
  test_requires_version("2.3.0")
  skip_databricks_connect()

  df <- tibble::tribble(
      ~row, ~name, ~var, ~value,
      1, "Sam", "age", 10,
      2, "Sam", "height", 1.5,
      3, "Bob", "age", 20
    )

  sdf <- copy_to(sc, df, overwrite = TRUE)

  df_pw <- df %>%
    pivot_wider(id_cols = name, names_from = var, values_from = value)

  sdf_pw <- sdf %>%
    pivot_wider(id_cols = name, names_from = var, values_from = value) %>%
    collect()

  expect_equal(
    df_pw$name,
    sdf_pw$name
  )

  expect_equal(
    df_pw$age,
    sdf_pw$age
  )

  expect_equal(
    df_pw$height,
    sdf_pw$height
  )
})

test_that("groups are processed the same as local", {

  test_requires_version("2.3.0")

  df <- tibble(
    x = c(rep("one", 4), rep("two", 4)),
    y = letters[1:8],
    z = 1:8
  )

  sdf <- copy_to(sc, df, overwrite = "TRUE")

  df_wide <- df %>%
    group_by(x) %>%
    pivot_wider(names_from = y, values_from = z, values_fill = 0) %>%
    select(x, letters[1:8])  %>%
    arrange(x) %>%
    ungroup()

  sdf_wide <- sdf %>%
    group_by(x) %>%
    pivot_wider(names_from = y, values_from = z, values_fill = 0) %>%
    collect() %>%
    select(x, letters[1:8]) %>%
    arrange(x)

  expect_equal(
    df_wide,
    sdf_wide
  )
})

test_that("values_fn can be a single function", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c(1, 10, 100))
  )

  expect_warning(
    pv <- tidyr::pivot_wider(
      sdf,
      names_from = key, values_from = val, values_fn = sum
    ) %>%
      collect() %>%
      dplyr::arrange(a)
  )

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
    tidyr::pivot_wider(names_from = names, values_from = value, values_fill = 0) %>%
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

test_that("default `names_from` and `values_from` works as expected", {
  test_requires_version("2.3.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(name = c("x", "y"), value = c(1, 2))
  )
  pv <- sdf %>%
    tidyr::pivot_wider() %>%
    collect()

  expect_equivalent(pv, tibble::tibble(x = 1, y = 2))
})



test_that("Simple cases work", {
  test_requires_version("2.3.0")

  d <- data.frame(
    c1 = c(11, 11, 11),
    c2 = c(21, 21, 21),
    c3 = c(31, 32, 33),
    c4 = c(41, 42, 43),
    c5 = c(51, 52, 53)
  )

  ds <- copy_to(sc, d)

  expect_equal(
    collect(pivot_wider(head(ds, 1), id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))),
    tidyr::pivot_wider(head(d, 1), id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))
  )

  expect_equal(
    collect(pivot_wider(head(ds, 2), id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))),
    tidyr::pivot_wider(head(d, 2), id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))
  )

  skip("Failing on GH, needs investigation")
  expect_equal(
    collect(pivot_wider(ds, id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))),
    tidyr::pivot_wider(d, id_cols = c(c1, c2), names_from = c3, values_from = c(c4, c5))
  )

})





