skip_connection("tidyr_nest")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()
simple_sdf_1 <- testthat_tbl(
  name = "tidyr_nest_simple_sdf_1",
  data = dplyr::tibble(x = c(1, 1, 1), y = 1:3)
)
simple_sdf_2 <- testthat_tbl(
  name = "tidyr_nest_simple_sdf_2",
  data = dplyr::tibble(x = 1:3, y = c("B", "A", "A"))
)
mtcars_tbl <- testthat_tbl("mtcars")

test_that("nest turns grouped values into one list-df", {
  test_requires_version("2.0.0")

  expect_warning_on_arrow(
    out <- tidyr::nest(simple_sdf_1, data = y) %>% collect()
  )

  expect_equivalent(
    out,
    dplyr::tibble(x = 1, data = list(lapply(seq(3), function(y) list(y = y))))
  )
})

test_that("nest uses grouping vars if present", {
  test_requires_version("2.0.0")

  out <- tidyr::nest(dplyr::group_by(simple_sdf_1, x))

  expect_equal(dplyr::group_vars(out), "x")

  expect_warning_on_arrow(
    o_c <- out %>%
      collect()
  )

  expect_equivalent(
    o_c,
    dplyr::tibble(x = 1, data = list(lapply(seq(3), function(y) list(y = y))))
  )
})

test_that("provided grouping vars override grouped defaults", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, dplyr::tibble(x = 1, y = 2, z = 3)) %>% dplyr::group_by(x)
  out <- sdf %>% tidyr::nest(data = y)

  expect_equal(dplyr::group_vars(out), "x")
  expect_equivalent(
    expect_warning_on_arrow(out %>% collect()),
    dplyr::tibble(x = 1, z = 3, data = list(list(y = 2)))
  )
})

test_that("no additional grouping var is created", {
  test_requires_version("2.0.0")

  mtcars_tbl_nested <- mtcars_tbl %>%
    dplyr::group_by(am) %>%
    tidyr::nest(perf = c(hp, mpg, disp, qsec))

  expect_equal(
    colnames(mtcars_tbl_nested),
    c("cyl", "drat", "wt", "vs", "am", "gear", "carb", "perf")
  )

  expect_equal(mtcars_tbl_nested %>% dplyr::group_vars(), "am")

  mtcars_tbl_nested <- mtcars_tbl %>%
    tidyr::nest(perf = c(hp, mpg, disp, qsec))

  expect_equal(
    colnames(mtcars_tbl_nested),
    c("cyl", "drat", "wt", "vs", "am", "gear", "carb", "perf")
  )
  expect_equal(mtcars_tbl_nested %>% dplyr::group_vars(), character())
})

test_that("puts data into the correct row", {
  test_requires_version("2.0.0")

  expect_warning_on_arrow(
    out <- tidyr::nest(simple_sdf_2, data = x) %>%
      dplyr::filter(y == "B") %>%
      collect()
  )

  expect_equivalent(
    out,
    dplyr::tibble(y = "B", data = list(list(x = 1)))
  )
})

test_that("nesting everything", {
  test_requires_version("2.0.0")

  expect_warning_on_arrow(
    out <- tidyr::nest(simple_sdf_2, data = c(x, y)) %>% collect()
  )

  expect_equivalent(
    out,
    dplyr::tibble(
      data = list(
        list(list(x = 1, y = "B"), list(x = 2, y = "A"), list(x = 3, y = "A"))
      )
    )
  )
})

test_that("can strip names", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, dplyr::tibble(x = c(1, 1, 1), ya = 1:3, yb = 4:6))

  expect_warning_on_arrow(
    out <- sdf %>%
      tidyr::nest(y = starts_with("y"), .names_sep = "") %>%
      collect()
  )

  expect_equivalent(
    out,
    dplyr::tibble(
      x = 1,
      y = list(list(list(a = 1, b = 4), list(a = 2, b = 5), list(a = 3, b = 6)))
    )
  )
})

test_that("nesting works for empty data frames", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, dplyr::tibble(x = integer(), y = character()))

  expect_warning_on_arrow(
    out <- tidyr::nest(sdf, data = x) %>% collect()
  )

  expect_named(out, c("y", "data"))
  expect_equal(nrow(out), 0L)

  expect_warning_on_arrow(
    out <- tidyr::nest(sdf, data = c(x, y)) %>% collect()
  )

  expect_named(out, "data")
  expect_equivalent(out, dplyr::tibble(data = list(NA)))
})

test_that("can nest multiple columns", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, dplyr::tibble(x = 1, a1 = 1, a2 = 2, b1 = 1, b2 = 2))

  expect_warning_on_arrow(
    out <- sdf %>%
      tidyr::nest(a = c(a1, a2), b = c(b1, b2)) %>%
      collect()
  )

  expect_equivalent(
    out,
    dplyr::tibble(
      x = 1,
      a = list(list(list(a1 = 1, a2 = 2))),
      b = list(list(list(b1 = 1, b2 = 2)))
    )
  )
})

test_that("nesting no columns nests all inputs", {
  test_requires_version("2.0.0")

  # included only for backward compatibility
  sdf <- copy_to(sc, dplyr::tibble(a1 = 1, a2 = 2, b1 = 1, b2 = 2))

  expect_warning(out <- tidyr::nest(sdf), "must not be empty")

  expect_equivalent(
    expect_warning_on_arrow(out %>% collect()),
    dplyr::tibble(
      data = list(list(list(a1 = 1, a2 = 2, b1 = 1, b2 = 2)))
    )
  )
})

test_that("can keep empty rows", {
  test_requires_version("2.0.0")

  sdf <- copy_to(
    sc,
    dplyr::tibble(
      a = seq(3),
      b = seq(3),
      c = seq(3),
      d = seq(3),
      e = seq(3)
    )
  )
  sdf.nested <- sdf %>%
    tidyr::nest(n1 = c(b, c), n2 = c(d, e)) %>%
    dplyr::mutate(
      n1 = dplyr::sql("IF(`a` == 1, NULL, `n1`)"),
      n2 = dplyr::sql("IF(`a` == 3, NULL, `n2`)")
    )

  expect_equivalent(
    sdf.nested %>%
      tidyr::unnest(c(n1, n2), keep_empty = TRUE) %>%
      collect() %>%
      dplyr::arrange(a),
    dplyr::tibble(
      a = seq(3),
      b = c(NA, 2, 3),
      c = c(NA, 2, 3),
      d = c(1, 2, NA),
      e = c(1, 2, NA)
    )
  )
})

test_that("bad inputs generate errors", {
  test_requires_version("2.0.0")

  expect_error(
    sdf_len(sc, 1) %>% tidyr::unnest(id),
    "`unnest.tbl_spark` is only supported for columns of type `array<struct<\\.\\*>>`"
  )
})

test_that("can unnest nested lists", {
  test_requires_version("2.4.0")

  tbl <- dplyr::tibble(
    a = c(1, 1, 2, 2, 3, 3, 3),
    b = lapply(seq(7), function(x) rep(1, x)),
    c = seq(7),
    d = lapply(seq(7), function(x) list(a = x, b = 2 * x, c = -x)),
    e = seq(-7, -1)
  )
  sdf <- copy_to(sc, tbl)
  sdf.nested <- sdf %>% tidyr::nest(n1 = c(b, c), n2 = c(d, e))

  expect_warning_on_arrow(
    s_n <- sdf.nested %>%
      tidyr::unnest(c(n1, n2)) %>%
      collect() %>%
      dplyr::arrange(c)
  )

  expect_equivalent(s_n, tbl)
})

test_that("grouping is preserved", {
  test_requires_version("2.0.0")

  sdf.nested <- copy_to(sc, dplyr::tibble(g = 1, x = seq(3))) %>%
    tidyr::nest(x = x) %>%
    dplyr::group_by(g)
  sdf <- sdf.nested %>% tidyr::unnest(x)

  expect_equal(dplyr::group_vars(sdf), "g")
})

test_that("handling names_sep correctly", {
  test_requires_version("2.0.0")

  tbl <- dplyr::tibble(
    a = seq(3),
    b = c(1, -1, 4),
    c = 1
  )
  sdf.nested <- copy_to(sc, tbl, overwrite = TRUE) %>%
    tidyr::nest(data = c(b, c))

  for (sep in c(".", "_")) {
    expect_equivalent(
      sdf.nested %>%
        tidyr::unnest(data, names_sep = sep) %>%
        collect() %>%
        dplyr::arrange(a),
      tbl %>%
        dplyr::mutate(data_b = b, data_c = c) %>%
        dplyr::select(a, data_b, data_c)
    )
  }
})

test_that("handling names_repair correctly", {
  test_requires_version("2.0.0")

  sdf.nested <- copy_to(
    sc,
    dplyr::tibble(
      a = seq(3),
      data_b = NA,
      data_c = NA,
      b = c(1, -1, 4),
      c = rep(1, 3)
    )
  ) %>%
    tidyr::nest(data = c(b, c))

  expect_error(
    sdf.nested %>%
      tidyr::unnest(data, names_sep = "_", names_repair = "check_unique"),
    class = "tibble_error_column_names_must_be_unique"
  )
  expect_equivalent(
    sdf.nested %>%
      tidyr::unnest(data, names_sep = "_", names_repair = "universal") %>%
      collect() %>%
      dplyr::arrange(a),
    dplyr::tibble(
      a = seq(3),
      data_b___2 = NA,
      data_c___3 = NA,
      data_b___4 = c(1, -1, 4),
      data_c___5 = 1
    )
  )
})

test_that("unnest() supports ptype", {
  test_requires_version("2.0.0")

  sdf.nested <- copy_to(
    sc,
    dplyr::tibble(g = c(1.0, 4.0, 9.0), x = seq(3))
  ) %>%
    tidyr::nest(x = x)

  expect_equivalent(
    tidyr::unnest(
      sdf.nested,
      x,
      ptype = dplyr::tibble(g = integer(), x = character())
    ) %>%
      collect() %>%
      dplyr::arrange(x),
    dplyr::tibble(g = c(1L, 4L, 9L), x = as.character(seq(3)))
  )
})

test_clear_cache()
