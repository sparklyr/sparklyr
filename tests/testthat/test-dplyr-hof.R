context("dplyr-hof")

test_requires("dplyr")

sc <- testthat_spark_connection()
test_tbl <- testthat_tbl(
  name = "hof_test_data",
  data = tibble::tibble(
    x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
    y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
    z = c(11, 12)
  )
)

single_col_tbl <- testthat_tbl(
  name = "hof_test_data_single_col",
  data = tibble::tibble(x = list(1:5, 6:10))
)

build_map_tbl <- function() {
  sdf_copy_to(
    sc,
    tibble::tibble(
      m1 = c("{\"1\":2,\"4\":3,\"6\":5}", "{\"2\":1,\"3\":4,\"8\":7}"),
      m2 = c("{\"2\":1,\"3\":4,\"8\":7}", "{\"6\":5,\"4\":3,\"1\":2}")
    ),
    overwrite = TRUE
  ) %>%
    dplyr::mutate(
      m1 = from_json(m1, "MAP<STRING, INT>"),
      m2 = from_json(m2, "MAP<STRING, INT>")
    )
}

build_map_zip_with_test_tbl <- function() {
  sdf_copy_to(
    sc,
    tibble::tibble(
      m1 = c("{\"1\":2,\"3\":4,\"5\":6}", "{\"2\":1,\"4\":3,\"6\":5}"),
      m2 = c("{\"1\":1,\"3\":3,\"5\":5}", "{\"2\":2,\"4\":4,\"6\":6}")
    ),
    overwrite = TRUE
  ) %>%
    dplyr::mutate(
      m1 = from_json(m1, "MAP<STRING, INT>"),
      m2 = from_json(m2, "MAP<STRING, INT>")
    )
}

map_tbl <- build_map_tbl()
map_zip_with_test_tbl <- build_map_zip_with_test_tbl()

if (spark_version(sc) >= "3.0.0") {
  map_tbl <- build_map_tbl()
}

test_that("'hof_transform' creating a new column", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = w, expr = x, func = x %->% (x * x)) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      w = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100))
    )
  )
})

test_that("'hof_transform' overwriting an existing column", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = x, expr = x, func = x %->% (x * x)) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_transform' works with array(...) expression", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = z, expr = array(z - 9, z - 8), func = x %->% (x * x)) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = list(c(4, 9), c(9, 16))
    )
  )
})

test_that("'hof_transform' works with formula", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = x, expr = x, func = ~ .x * .x) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_transform' works with default args", {
  test_requires_version("2.4.0")

  sq <- single_col_tbl %>%
    hof_transform(~ .x * .x) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100)),
    )
  )
})

test_that("transform() works through dbplyr", {
  test_requires_version("2.4.0")

  for (fn in list(x %->% (x * x), ~ .x * .x)) {
    sq <- test_tbl %>%
      dplyr::mutate(w = transform(x, fn)) %>%
      collect()

    expect_equivalent(
      sq,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        w = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100))
      )
    )
  }
})

test_that("'hof_filter' creating a new column", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = mod_3_is_0_or_1, expr = x, func = x %->% (x %% 3 != 2)) %>%
    collect()

  expect_equivalent(
    filtered,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      mod_3_is_0_or_1 = list(c(1, 3, 4), c(6, 7, 9, 10))
    )
  )
})

test_that("'hof_filter' overwriting an existing column", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = x, expr = x, func = x %->% (x %% 3 != 2)) %>%
    collect()

  expect_equivalent(
    filtered,
    tibble::tibble(
      x = list(c(1, 3, 4), c(6, 7, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_filter' works with array(...) expression", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = z, expr = array(8, z - 1, z + 1), func = x %->% (x %% 3 == 2)) %>%
    collect()

  expect_equivalent(
    filtered,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = list(c(8), c(8, 11))
    )
  )
})

test_that("'hof_filter' works with formula", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = x, expr = x, func = ~ .x %% 3 != 2) %>%
    collect()

  expect_equivalent(
    filtered,
    tibble::tibble(
      x = list(c(1, 3, 4), c(6, 7, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_filter' works with default args", {
  test_requires_version("2.4.0")

  sq <- single_col_tbl %>%
    hof_filter(~ .x %% 2 == 1) %>%
    collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 3, 5), c(7, 9)),
    )
  )
})

test_that("filter() works through dbplyr", {
  test_requires_version("2.4.0")

  for (fn in list(x %->% (x %% 3 != 2), ~ .x %% 3 != 2)) {
    filtered <- test_tbl %>%
      dplyr::mutate(mod_3_is_0_or_1 = filter(x, fn)) %>%
      collect()

    expect_equivalent(
      filtered,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        mod_3_is_0_or_1 = list(c(1, 3, 4), c(6, 7, 9, 10))
      )
    )
  }
})

test_that("'hof_aggregate' creating a new column", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = sum,
      expr = x,
      start = z,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
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
      start = z,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = c(26, 52),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_aggregate' works with array(...) expression", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = sum,
      expr = array(1, 2, z, 3),
      start = z,
      merge = .(sum_so_far, num) %->% (sum_so_far + num)
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
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
      start = z,
      merge = .(sum_so_far, num) %->% (sum_so_far + num),
      finish = sum %->% (sum * sum)
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = c(676, 2704),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_aggregate' can apply formula as 'finish' transformation", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = x,
      expr = x,
      start = z,
      merge = ~ .x + .y,
      finish = ~ .x * .x
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = c(676, 2704),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_aggregate' works with formula", {
  test_requires_version("2.4.0")

  agg <- test_tbl %>%
    hof_aggregate(
      dest_col = x,
      expr = x,
      start = z,
      merge = ~ .x + .y
    ) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(
      x = c(26, 52),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_aggregate' works with default args", {
  test_requires_version("2.4.0")

  agg <- single_col_tbl %>%
    hof_aggregate("-", ~ CONCAT(.y, .x), ~ CONCAT("(", .x, ")")) %>%
    collect()

  expect_equivalent(
    agg,
    tibble::tibble(x = c("(54321-)", "(109876-)"))
  )
})

test_that("aggregate() works through dbplyr", {
  test_requires_version("2.4.0")

  for (merge in list(.(sum_so_far, num) %->% (sum_so_far + num), ~.x + .y)) {
    agg <- test_tbl %>%
      dplyr::mutate(sum = aggregate(x, z, merge)) %>%
      collect()

    expect_equivalent(
      agg,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        sum = c(26, 52)
      )
    )

    for (finish in list(sum %->% (sum * sum), ~.x * .x)) {
      agg <- test_tbl %>%
        dplyr::mutate(x = aggregate(x, z, merge, finish)) %>%
        collect()

       expect_equivalent(
         agg,
         tibble::tibble(
           x = c(676, 2704),
           y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
           z = c(11, 12)
         )
       )
    }
  }
})

test_that("'hof_exists' creating a new column", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = found,
      expr = x,
      pred = num %->% (num == 5)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
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
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(TRUE, FALSE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_exists' works with array(...) expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, z, 13, 14),
      pred = num %->% (num == 12)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(FALSE, TRUE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, z, 13, 14),
      pred = num %->% (num == 10)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(TRUE, TRUE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = array(10, z, 13, 14),
      pred = num %->% (num == 17)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(FALSE, FALSE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_exists' works with formula", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_exists(
      dest_col = x,
      expr = x,
      pred = ~ .x == 5
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(TRUE, FALSE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_exists' works with default args", {
  test_requires_version("2.4.0")

  res <- single_col_tbl %>%
    hof_exists(~ .x == 5) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(x = c(TRUE, FALSE))
  )
})

test_that("exists() works through dbplyr", {
  test_requires_version("2.4.0")

  for (pred in list(num %->% (num == 5), ~ .x == 5)) {
    res <- test_tbl %>%
      dplyr::mutate(found = exists(x, pred)) %>%
      collect()

    expect_equivalent(
      res,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        found = c(TRUE, FALSE)
      )
    )
  }
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
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
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
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_zip_with' works with array(...) expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(
      dest_col = x,
      left = array(3, 1, z, 4),
      right = array(2, z, 5, 17),
      func = .(x, y) %->% (x * y)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(6, 11, 55, 68), c(6, 12, 60, 68)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_zip_with' works with formula", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(
      dest_col = x,
      left = x,
      right = y,
      func = ~ .x * .y
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_zip_with' works with default args", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    hof_zip_with(~ .x * .y) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80))
    )
  )
})

test_that("zip_with() works through dbplyr", {
  test_requires_version("2.4.0")

  for (func in list(.(x, y) %->% (x * y), ~ .x * .y)) {
    res <- test_tbl %>%
      dplyr::mutate(product = zip_with(x, y, func)) %>%
      collect()

    expect_equivalent(
      res,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        product = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80))
      )
    )
  }
})

test_that("'hof_array_sort' creating a new column", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_array_sort(
      func = .(x, y) %->% (as.integer(sign(y - x))),
      expr = x,
      dest_col = sorted_x
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      sorted_x = list(c(5, 4, 3, 2, 1), c(10, 9, 8, 7, 6))
    )
  )
})

test_that("'hof_array_sort' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_array_sort(
      func = .(x, y) %->% (as.integer(sign(y - x))),
      expr = x
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(5, 4, 3, 2, 1), c(10, 9, 8, 7, 6)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
    )
  )
})

test_that("'hof_array_sort' works with array(...) expression", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_array_sort(
      func = .(x, y) %->% (as.integer(sign(y - x))),
      expr = array(z + 1, z + 3),
      dest_col = sorted_arr
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      sorted_arr = list(c(14, 12), c(15, 13))
    )
  )
})

test_that("'hof_array_sort' works with formula", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_array_sort(
      func = ~ as.integer(sign(.y - .x)),
      expr = x,
      dest_col = sorted_x
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      sorted_x = list(c(5, 4, 3, 2, 1), c(10, 9, 8, 7, 6))
    )
  )
})

test_that("'hof_array_sort' works with default args", {
  test_requires_version("3.0.0")

  res <- single_col_tbl %>%
    hof_array_sort(~ as.integer(sign(.y - .x))) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(5:1, 10:6)
    )
  )
})

test_that("array_sort() works through dbplyr", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    dplyr::mutate(y = array_sort(y)) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 2, 4, 5, 8), c(1, 2, 4, 7, 8)),
      z = c(11, 12)
    )
  )

  for (fn in list(
                  .(x, y) %->% (as.integer(sign(y - x))),
                  ~ as.integer(sign(.y - .x)))
  ) {
    res <- test_tbl %>%
      dplyr::mutate(y = array_sort(y, fn)) %>%
      collect()

    expect_equivalent(
      res,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(8, 5, 4, 2, 1), c(8, 7, 4, 2, 1)),
        z = c(11, 12)
      )
    )
  }
})

test_that("'hof_map_filter' creating a new column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_map_filter(
      func = .(x, y) %->% (as.integer(x) > y),
      expr = m1,
      dest_col = filtered_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      filtered_m1 = to_json(filtered_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$filtered_m1[[1]]), c("4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$filtered_m1[[2]]), c("2" = 1, "8" = 7))
})

test_that("'hof_map_filter' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_map_filter(
      func = .(x, y) %->% (as.integer(x) > y),
      expr = m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
})

test_that("'hof_map_filter' works with map(...) expression", {
  test_requires_version("3.0.0")

  res <- sdf_len(sc, 1) %>%
    hof_map_filter(
      func = .(x, y) %->% (as.integer(x) > y),
      expr = map(1, 2, 4, 3, 7, 8, 6, 5),
      dest_col = m
    ) %>%
    dplyr::mutate(m = to_json(m)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m), c("4" = 3, "6" = 5))
})

test_that("'hof_map_filter' works with formula", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_map_filter(
      func = ~ as.integer(.x) > .y,
      expr = m1,
      dest_col = filtered_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      filtered_m1 = to_json(filtered_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$filtered_m1[[1]]), c("4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$filtered_m1[[2]]), c("2" = 1, "8" = 7))
})

test_that("'hof_map_filter' works with default args", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_map_filter(~ as.integer(.x) > .y) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3))
})

test_that("map_filter() works through dbplyr", {
  test_requires_version("3.0.0")

  for (fn in list(.(x, y) %->% (as.integer(x) > y), ~ as.integer(.x) > .y)) {
    res <- map_tbl %>%
      dplyr::mutate(filtered_m1 = map_filter(m1, fn)) %>%
      dplyr::mutate(
        m1 = to_json(m1),
        m2 = to_json(m2),
        filtered_m1 = to_json(filtered_m1)
      ) %>%
      collect()

    expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
    expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
    expect_equivalent(rjson::fromJSON(res$filtered_m1[[1]]), c("4" = 3, "6" = 5))
    expect_equivalent(rjson::fromJSON(res$filtered_m1[[2]]), c("2" = 1, "8" = 7))
  }
})

test_that("'hof_forall' creating a new column", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_forall(
      pred = x %->% (x != 5),
      expr = x,
      dest_col = does_not_contain_5
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      does_not_contain_5 = c(FALSE, TRUE)
    )
  )
})

test_that("'hof_forall' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_forall(
      pred = x %->% (x != 5),
      expr = x
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = c(FALSE, TRUE),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12)
    )
  )
})

test_that("'hof_forall' works with array(...) expression", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_forall(
      pred = x %->% (x != 5),
      expr = array(z - 8, z - 7),
      dest_col = does_not_contain_5
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      does_not_contain_5 = c(TRUE, FALSE)
    )
  )
})

test_that("'hof_forall' works with formula", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_forall(
      pred = ~ .x != 5,
      expr = array(z - 8, z - 7),
      dest_col = does_not_contain_5
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      does_not_contain_5 = c(TRUE, FALSE)
    )
  )
})

test_that("'hof_forall' works with default args", {
  test_requires_version("3.0.0")

  res <- single_col_tbl %>%
    hof_forall(~ .x != 5) %>%
    collect()

  expect_equivalent(res, tibble::tibble(x = c(FALSE, TRUE)))
})

test_that("forall() works through dbplyr", {
  test_requires_version("3.0.0")

  for (pred in list(x %->% (x != 5), ~ .x != 5)) {
    res <- test_tbl %>%
      dplyr::mutate(does_not_contain_5 = forall(x, pred)) %>%
      collect()

    expect_equivalent(
      res,
      tibble::tibble(
        x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
        y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
        z = c(11, 12),
        does_not_contain_5 = c(FALSE, TRUE)
      )
    )
  }
})

test_that("'hof_transform_keys' creating a new column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_keys(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = m1,
      dest_col = transformed_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      transformed_m1 = to_json(transformed_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c(k_1_v_2 = 2, k_4_v_3 = 3, k_6_v_5 = 5))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c(k_2_v_1 = 1, k_3_v_4 = 4, k_8_v_7 = 7))
})

test_that("'hof_transform_keys' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_keys(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c(k_1_v_2 = 2, k_4_v_3 = 3, k_6_v_5 = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c(k_2_v_1 = 1, k_3_v_4 = 4, k_8_v_7 = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
})

test_that("'hof_transform_keys' works with map(...) expression", {
  test_requires_version("3.0.0")

  res <- sdf_len(sc, 1) %>%
    hof_transform_keys(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = map(1, 2, 4, 3, 7, 8, 6, 5),
      dest_col = m
    ) %>%
    dplyr::mutate(m = to_json(m)) %>%
    collect()

  expect_equivalent(
    rjson::fromJSON(res$m),
    c(k_1_v_2 = 2, k_4_v_3 = 3, k_7_v_8 = 8, k_6_v_5 = 5)
  )
})

test_that("'hof_transform_keys' works with formula", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_keys(
      func = ~ CONCAT("k_", .x, "_v_", .y),
      expr = m1,
      dest_col = transformed_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      transformed_m1 = to_json(transformed_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c(k_1_v_2 = 2, k_4_v_3 = 3, k_6_v_5 = 5))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c(k_2_v_1 = 1, k_3_v_4 = 4, k_8_v_7 = 7))
})

test_that("'hof_transform_keys' works with default args", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_keys(~ CONCAT("k_", .x, "_v_", .y)) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c(k_2_v_1 = 1, k_3_v_4 = 4, k_8_v_7 = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c(k_6_v_5 = 5, k_4_v_3 = 3, k_1_v_2 = 2))
})

test_that("transform_keys() works through dbplyr", {
  test_requires_version("3.0.0")

  for (fn in list(
                  .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
                  ~ CONCAT("k_", .x, "_v_", .y))
  ) {
    res <- map_tbl %>%
      dplyr::mutate(
        transformed_m1 = transform_keys(m1, fn)
      ) %>%
      dplyr::mutate(
        m1 = to_json(m1),
        m2 = to_json(m2),
        transformed_m1 = to_json(transformed_m1)
      ) %>%
      collect()

    expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
    expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
    expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c(k_1_v_2 = 2, k_4_v_3 = 3, k_6_v_5 = 5))
    expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c(k_2_v_1 = 1, k_3_v_4 = 4, k_8_v_7 = 7))
  }
})

test_that("'hof_transform_values' creating a new column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_values(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = m1,
      dest_col = transformed_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      transformed_m1 = to_json(transformed_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c("1" = "k_1_v_2", "4" = "k_4_v_3", "6" = "k_6_v_5"))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c("2" = "k_2_v_1", "3" = "k_3_v_4", "8" = "k_8_v_7"))
})

test_that("'hof_transform_values' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_values(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = "k_1_v_2", "4" = "k_4_v_3", "6" = "k_6_v_5"))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = "k_2_v_1", "3" = "k_3_v_4", "8" = "k_8_v_7"))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
})

test_that("'hof_transform_values' works with map(...) expression", {
  test_requires_version("3.0.0")

  res <- sdf_len(sc, 1) %>%
    hof_transform_values(
      func = .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
      expr = map(1L, 2L, 4L, 3L, 7L, 8L, 6L, 5L),
      dest_col = m
    ) %>%
    dplyr::mutate(m = to_json(m)) %>%
    collect()

  expect_equivalent(
    rjson::fromJSON(res$m),
    c("1" = "k_1_v_2", "4" = "k_4_v_3", "7" = "k_7_v_8", "6" = "k_6_v_5")
  )
})

test_that("'hof_transform_values' works with formula", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_transform_values(
      func = ~ CONCAT("k_", .x, "_v_", .y),
      expr = m1,
      dest_col = transformed_m1
    ) %>%
    dplyr::mutate(
      m1 = to_json(m1),
      m2 = to_json(m2),
      transformed_m1 = to_json(transformed_m1)
    ) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c("1" = "k_1_v_2", "4" = "k_4_v_3", "6" = "k_6_v_5"))
  expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c("2" = "k_2_v_1", "3" = "k_3_v_4", "8" = "k_8_v_7"))
})

test_that("'hof_transform_values' works with default args", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    dplyr::select(m2) %>%
    hof_transform_values(~ CONCAT("k_", .x, "_v_", .y)) %>%
    dplyr::mutate(m2 = to_json(m2)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = "k_2_v_1", "3" = "k_3_v_4", "8" = "k_8_v_7"))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = "k_6_v_5", "4" = "k_4_v_3", "1" = "k_1_v_2"))
})

test_that("transform_values() works through dbplyr", {
  test_requires_version("3.0.0")

  for (fn in list(
                  .(x, y) %->% (CONCAT("k_", x, "_v_", y)),
                  ~ CONCAT("k_", .x, "_v_", .y))
  ) {
    res <- map_tbl %>%
      dplyr::mutate(transformed_m1 = transform_values(m1, fn)) %>%
      dplyr::mutate(
        m1 = to_json(m1),
        m2 = to_json(m2),
        transformed_m1 = to_json(transformed_m1)
      ) %>%
      collect()

    expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
    expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "3" = 4, "8" = 7))
    expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3, "1" = 2))
    expect_equivalent(rjson::fromJSON(res$transformed_m1[[1]]), c("1" = "k_1_v_2", "4" = "k_4_v_3", "6" = "k_6_v_5"))
    expect_equivalent(rjson::fromJSON(res$transformed_m1[[2]]), c("2" = "k_2_v_1", "3" = "k_3_v_4", "8" = "k_8_v_7"))
  }
})

test_that("'hof_map_zip_with' creating a new column", {
  test_requires_version("3.0.0")

  res <- map_zip_with_test_tbl %>%
    hof_map_zip_with(
      func = .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2)),
      map1 = m1,
      map2 = m2,
      dest_col = z
    ) %>%
    dplyr::mutate(m1 = to_json(m1), m2 = to_json(m2), z = to_json(z)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "3" = 4, "5" = 6))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = 1, "3" = 3, "5" = 5))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("2" = 2, "4" = 4, "6" = 6))
  expect_equivalent(rjson::fromJSON(res$z[[1]]), c("1" = "1_2_1", "3" = "3_4_3", "5" = "5_6_5"))
  expect_equivalent(rjson::fromJSON(res$z[[2]]), c("2" = "2_1_2", "4" = "4_3_4", "6" = "6_5_6"))
})

test_that("'hof_map_zip_with' overwriting an existing column", {
  test_requires_version("3.0.0")

  res <- map_zip_with_test_tbl %>%
    hof_map_zip_with(
      func = .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2)),
      map1 = m1,
      map2 = m2
    ) %>%
    dplyr::mutate(m1 = to_json(m1), m2 = to_json(m2)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "3" = 4, "5" = 6))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = "1_2_1", "3" = "3_4_3", "5" = "5_6_5"))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("2" = "2_1_2", "4" = "4_3_4", "6" = "6_5_6"))
})

test_that("'hof_map_zip_with' works with map(...) expression", {
  test_requires_version("3.0.0")

  res <- sdf_len(sc, 1) %>%
    hof_map_zip_with(
      func = .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2)),
      map1 = map(1L, 2L, 4L, 3L, 7L, 8L, 6L, 5L),
      map2 = map(1L, 1L, 4L, 4L, 7L, 7L, 6L, 6L),
      dest_col = m
    ) %>%
    dplyr::select(m) %>%
    dplyr::mutate(m = to_json(m)) %>%
    collect()

  expect_equivalent(
    rjson::fromJSON(res$m),
    c("1" = "1_2_1", "4" = "4_3_4", "7" = "7_8_7", "6" = "6_5_6")
  )
})

test_that("'hof_map_zip_with' works with formula", {
  test_requires_version("3.0.0")

  res <- map_zip_with_test_tbl %>%
    hof_map_zip_with(
      func = ~ CONCAT(.x, "_", .y, "_", .z),
      map1 = m1,
      map2 = m2
    ) %>%
    dplyr::mutate(m1 = to_json(m1), m2 = to_json(m2)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "3" = 4, "5" = 6))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = "1_2_1", "3" = "3_4_3", "5" = "5_6_5"))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("2" = "2_1_2", "4" = "4_3_4", "6" = "6_5_6"))
})

test_that("'hof_map_zip_with' works with default args", {
  test_requires_version("3.0.0")

  res <- map_zip_with_test_tbl %>%
    hof_map_zip_with(func = .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2))) %>%
    dplyr::mutate(m1 = to_json(m1), m2 = to_json(m2)) %>%
    collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "3" = 4, "5" = 6))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = "1_2_1", "3" = "3_4_3", "5" = "5_6_5"))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("2" = "2_1_2", "4" = "4_3_4", "6" = "6_5_6"))
})

test_that("map_zip_with() works through dbplyr", {
  test_requires_version("3.0.0")

  for (fn in list(
                  .(k, v1, v2) %->% (CONCAT(k, "_", v1, "_", v2)),
                  ~ CONCAT(.x, "_", .y, "_", .z))
  ) {
    res <- map_zip_with_test_tbl %>%
      dplyr::mutate(z = map_zip_with(m1, m2, fn)) %>%
      dplyr::mutate(m1 = to_json(m1), m2 = to_json(m2), z = to_json(z)) %>%
      collect()

    expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "3" = 4, "5" = 6))
    expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "4" = 3, "6" = 5))
    expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("1" = 1, "3" = 3, "5" = 5))
    expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("2" = 2, "4" = 4, "6" = 6))
    expect_equivalent(rjson::fromJSON(res$z[[1]]), c("1" = "1_2_1", "3" = "3_4_3", "5" = "5_6_5"))
    expect_equivalent(rjson::fromJSON(res$z[[2]]), c("2" = "2_1_2", "4" = "4_3_4", "6" = "6_5_6"))
  }
})

test_that("accessing struct field inside lambda expression", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    dplyr::mutate(array_of_structs = array(struct(z), named_struct("z", -1))) %>%
    hof_transform(
      dest_col = w,
      expr = array_of_structs,
      func = s %->% (s$z)
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      array_of_structs = list(list(list(z = 11), list(z = -1)), list(list(z = 12), list(z = -1))),
      w = list(c(11, -1), c(12, -1))
    )
  )
})

test_that("accessing struct field inside formula", {
  test_requires_version("2.4.0")

  res <- test_tbl %>%
    dplyr::mutate(array_of_structs = array(struct(z), named_struct("z", -1))) %>%
    hof_transform(
      dest_col = w,
      expr = array_of_structs,
      func = ~ .x$z
    ) %>%
    collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = c(11, 12),
      array_of_structs = list(list(list(z = 11), list(z = -1)), list(list(z = 12), list(z = -1))),
      w = list(c(11, -1), c(12, -1))
    )
  )
})
