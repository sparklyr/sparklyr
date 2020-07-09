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
  df <- tibble::tibble(
    m1 = c("{\"1\":2,\"4\":3,\"6\":5}", "{\"2\":1,\"3\":4,\"8\":7}"),
    m2 = c("{\"2\":1,\"3\":4,\"8\":7}", "{\"6\":5,\"4\":3,\"1\":2}")
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    dplyr::mutate(m1 = from_json(m1, "MAP<STRING, INT>"),
                  m2 = from_json(m2, "MAP<STRING, INT>"))

  sdf
}

if (spark_version(sc) >= "3.0.0")
  map_tbl <- build_map_tbl()

test_that("'hof_transform' creating a new column", {
  test_requires_version("2.4.0")

  sq <- test_tbl %>%
    hof_transform(dest_col = w, expr = x, func = x %->% (x * x)) %>%
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 4, 9, 16, 25), c(36, 49, 64, 81, 100)),
    )
  )
})

test_that("'hof_filter' creating a new column", {
  test_requires_version("2.4.0")

  filtered <- test_tbl %>%
    hof_filter(dest_col = mod_3_is_0_or_1, expr = x, func = x %->% (x %% 3 != 2)) %>%
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    sq,
    tibble::tibble(
      x = list(c(1, 3, 5), c(7, 9)),
    )
  )
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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    agg,
    tibble::tibble(x = c("(54321-)", "(109876-)"))
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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    res,
    tibble::tibble(x = c(TRUE, FALSE))
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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(c(1, 2, 3, 4, 5), c(6, 7, 8, 9, 10)),
      y = list(c(1, 4, 2, 8, 5), c(7, 1, 4, 2, 8)),
      z = list(c(1, 8, 6, 32, 25), c(42, 7, 32, 18, 80))
    )
  )
})

test_that("'hof_array_sort' creating a new column", {
  test_requires_version("3.0.0")

  res <- test_tbl %>%
    hof_array_sort(
      func = .(x, y) %->% (as.integer(sign(y - x))),
      expr = x,
      dest_col = sorted_x
    ) %>%
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

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
    sdf_collect()

  expect_equivalent(
    res,
    tibble::tibble(
      x = list(5:1, 10:6)
    )
  )
})

test_that("'hof_map_filter' creating a new column", {
  test_requires_version("3.0.0")

  res <- map_tbl %>%
    hof_map_filter(
      func = .(x, y) %->% (as.integer(x) > y),
      expr = m1,
      dest_col = filtered_m1
    ) %>%
    dplyr::mutate(m1 = to_json(m1),
                  m2 = to_json(m2),
                  filtered_m1 = to_json(filtered_m1)) %>%
    sdf_collect()

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
    dplyr::mutate(m1 = to_json(m1),
                  m2 = to_json(m2)) %>%
    sdf_collect()

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
    sdf_collect()

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
    dplyr::mutate(m1 = to_json(m1),
                  m2 = to_json(m2),
                  filtered_m1 = to_json(filtered_m1)) %>%
    sdf_collect()

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
    dplyr::mutate(m1 = to_json(m1),
                  m2 = to_json(m2)) %>%
    sdf_collect()

  expect_equivalent(rjson::fromJSON(res$m1[[1]]), c("1" = 2, "4" = 3, "6" = 5))
  expect_equivalent(rjson::fromJSON(res$m1[[2]]), c("2" = 1, "3" = 4, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[1]]), c("2" = 1, "8" = 7))
  expect_equivalent(rjson::fromJSON(res$m2[[2]]), c("6" = 5, "4" = 3))
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
    sdf_collect()

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
    sdf_collect()

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
