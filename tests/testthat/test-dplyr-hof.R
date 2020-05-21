context("Spark SQL higher-order function wrappers for dplyr")

test_requires("dplyr")

sc <- testthat_spark_connection()
test_tbl <- testthat_tbl(
  name = "hof_test_data",
  data = tibble::tibble(
    v = c(11, 12),
    x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
    y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
  )
)

expect_data_equal <- function(actual, expected) {
  expect_equal(colnames(actual), colnames(expected))
  for (col in colnames(expected))
    expect_equal(actual[[col]], expected[[col]])
}

test_that("'hof_transform' creating a new column", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = z, expr = x, func = x %->% (x * x)) %>%
    sdf_collect()

  expect_data_equal(
    sq,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100))
    )
  )
})

test_that("'hof_transform' overwriting an existing column", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = x, expr = x, func = x %->% (x * x)) %>%
    sdf_collect()

  expect_data_equal(
    sq,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_transform' works with array(...) expression", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = v, expr = array(v - 9, v - 8), func = x %->% (x * x)) %>%
    sdf_collect()

  expect_data_equal(
    sq,
    tibble::tibble(
      v = list(c(4, 9), c(9, 16)),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_filter' creating a new column", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = mod_3_is_0_or_1, expr = x, func = x %->% (x %% 3 != 2)) %>%
    sdf_collect()

  expect_data_equal(
    filtered,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      mod_3_is_0_or_1 = list(c(1, 3, 4), c(6, 7, 9, 10))
    )
  )
})

test_that("'hof_filter' overwriting an existing column", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = x, expr = x, func = x %->% (x %% 3 != 2)) %>%
    sdf_collect()

  expect_data_equal(
    filtered,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 3, 4), c(6, 7, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_filter' works with array(...) expression", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = v, expr = array(8, v - 1, v + 1), func = x %->% (x %% 3 == 2)) %>%
    sdf_collect()

  expect_data_equal(
    filtered,
    tibble::tibble(
      v = list(c(8), c(8, 11)),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_aggregate' creating a new column", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = sum,
      expr = x,
      start = v,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    sdf_collect()

  expect_data_equal(
    agg,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      sum = c(26, 52)
    )
  )
})

test_that("'hof_aggregate' overwriting an existing column", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = x,
      expr = x,
      start = v,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    sdf_collect()

  expect_data_equal(
    agg,
    tibble::tibble(
      v = c(11, 12),
      x = c(26, 52),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_aggregate' works with array(...) expression", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = sum,
      expr = array(1, 2, v, 3),
      start = v,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    sdf_collect()

  expect_data_equal(
    agg,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      sum = c(28, 30)
    )
  )
})

test_that("'hof_aggregate' applies 'finish' transformation correctly", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = x,
      expr = x,
      start = v,
      merge = .(sum_so_far, num) %->% (sum_so_far + num),
      finish = sum %->% (sum * sum)
    ) %>%
    sdf_collect()

  expect_data_equal(
    agg,
    tibble::tibble(
      v = c(11, 12),
      x = c(676, 2704),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_exists' creating a new column", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = found,
      expr = x,
      pred = num %->% (num == 5)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      found = c(TRUE, FALSE)
    )
  )
})

test_that("'hof_exists' overwriting an existing column", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = x,
      pred = num %->% (num == 5)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = c(TRUE, FALSE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
    )
  )
})

test_that("'hof_exists' works with array(...) expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, v, 13, 14),
      pred = num %->% (num == 12)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = c(FALSE, TRUE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
    )
  )

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, v, 13, 14),
      pred = num %->% (num == 10)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = c(TRUE, TRUE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, v, 13, 14),
      pred = num %->% (num == 17)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = c(FALSE, FALSE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_zip_with' creating a new column", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(
      dest_col = product,
      left = x,
      right = y,
      func = .(x, y) %->% (x * y)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      product = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80))
    )
  )
})

test_that("'hof_zip_with' overwriting an existing column", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(
      dest_col = x,
      left = x,
      right = y,
      func = .(x, y) %->% (x * y)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("'hof_zip_with' works with array(...) expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(
      dest_col = x,
      left = array(3, 1, v, 4),
      right = array(2, v, 5, 17),
      func = .(x, y) %->% (x * y)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(6, 11, 55, 68), c(6, 12, 60, 68)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8))
    )
  )
})

test_that("accessing struct field inside lambda expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    dplyr::mutate(array_of_structs = array(struct(v), named_struct("v", -1))) %>%
    hof_transform(
      dest_col = w,
      expr = array_of_structs,
      func = s %->% (s$v)
    ) %>%
    sdf_collect()

  expect_data_equal(
    res,
    tibble::tibble(
      v = c(11, 12),
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      array_of_structs = list(list(list(v = 11), list(v = -1)), list(list(v = 12), list(v = -1))),
      w = list(c(11, -1), c(12, -1)),
    )
  )
})
