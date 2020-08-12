context("tidyr-unnest")

sc <- testthat_spark_connection()

test_that("can keep empty rows", {
  test_requires_version("2.0.0")

  sdf <- copy_to(
    sc,
    tibble::tibble(
      a = seq(3), b = seq(3), c = seq(3), d = seq(3), e = seq(3)
    )
  )
  sdf.nested <- sdf %>% tidyr::nest(n1 = c(b, c), n2 = c(d, e)) %>%
    dplyr::mutate(
      n1 = dplyr::sql("IF(`a` == 1, NULL, `n1`)"),
      n2 = dplyr::sql("IF(`a` == 3, NULL, `n2`)")
    )

  expect_equivalent(
    sdf.nested %>%
      tidyr::unnest(c(n1, n2), keep_empty = TRUE) %>%
      collect() %>%
      dplyr::arrange(a),
    tibble::tibble(
      a = seq(3), b = c(NA, 2, 3), c = c(NA, 2, 3), d = c(1, 2, NA), e = c(1, 2, NA)
    )
  )
})

test_that("bad inputs generate errors", {
  expect_error(
    sdf_len(sc, 1) %>% tidyr::unnest(id),
    "Unnesting is only supported for struct type columns\\."
  )
})

test_that("can unnest nested lists", {
  tbl <- tibble::tibble(
    a = c(1, 1, 2, 2, 3, 3, 3),
    b = lapply(seq(7), function(x) rep(1, x)),
    c = seq(7),
    d = lapply(seq(7), function(x) list(a = x, b = 2 * x, c = -x)),
    e = seq(-7, -1)
  )
  sdf <- copy_to(sc, tbl)
  sdf.nested <- sdf %>% tidyr::nest(n1 = c(b, c), n2 = c(d, e))

  expect_equivalent(
    sdf.nested %>%
      tidyr::unnest(c(n1, n2)) %>%
      collect() %>%
      dplyr::arrange(c),
    tbl
  )
})

test_that("grouping is preserved", {
  sdf.nested <- copy_to(sc, tibble::tibble(g = 1, x = seq(3))) %>%
    tidyr::nest(x = x) %>%
    dplyr::group_by(g)
  sdf <- sdf.nested %>% tidyr::unnest(x)

  expect_equal(dplyr::group_vars(sdf), "g")
})

test_that("handling names_sep correctly", {
  tbl <- tibble::tibble(
    a = seq(3), b = c(1, -1, 4), c = 1
  )
  sdf.nested <- copy_to(sc, tbl, overwrite = TRUE) %>%
    tidyr::nest(data = c(b, c))

  for (sep in c(".", "_")) {
    expect_equivalent(
      sdf.nested %>%
        tidyr::unnest(data, names_sep = sep) %>%
        collect() %>%
        dplyr::arrange(a),
      tbl %>% dplyr::mutate(data_b = b, data_c = c) %>%
        dplyr::select(a, data_b, data_c)
    )
  }
})

test_that("handling names_repair correctly", {
  sdf.nested <- copy_to(
    sc,
    tibble::tibble(
      a = seq(3), data_b = NA, data_c = NA, b = c(1, -1, 4), c = rep(1, 3)
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
    tibble::tibble(
      a = seq(3),
      data_b___2 = NA,
      data_c___3 = NA,
      data_b___4 = c(1, -1, 4),
      data_c___5 = 1
    )
  )
})
