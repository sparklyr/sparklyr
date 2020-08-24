context("tidyr-pivot-longer")

sc <- testthat_spark_connection()

test_that("can pivot all cols to long", {
  sdf <- copy_to(sc, tibble::tibble(x = 1:2, y = 3:4))
  pv <- tidyr::pivot_longer(sdf, x:y) %>% collect()

  expect_equivalent(
    pv,
    tibble::tibble(
      name = c("x", "y", "x", "y"),
      value = c(1, 3, 2, 4)
    )
  )
})

test_that("values interleaved correctly", {
  sdf <- copy_to(
    sc,
    tibble::tibble(x = c(1, 2), y = c(10, 20), z = c(100, 200))
  )
  pv <- tidyr::pivot_longer(sdf, 1:3) %>% collect()

  expect_equivalent(
    pv,
    tibble::tibble(
      name = c("x", "y", "z", "x", "y", "z"),
      value = c(1, 10, 100, 2, 20, 200)
    )
  )
})

test_that("can drop missing values", {
  sdf <- copy_to(sc, tibble::tibble(x = c(1, NA), y = c(NA, 2)))
  pv <- tidyr::pivot_longer(sdf, x:y, values_drop_na = TRUE) %>% collect()

  expect_equivalent(pv, tibble::tibble(name = c("x", "y"), value = c(1, 2)))
})

test_that("preserves original keys", {
  sdf <- copy_to(sc, tibble::tibble(x = 1:2, y = 2L, z = 1:2))
  pv <- tidyr::pivot_longer(sdf, y:z) %>% collect()

  expect_equivalent(
    pv,
    tibble::tibble(
      x = rep(1:2, each = 2),
      name = c("y", "z", "y", "z"),
      value = c(2, 1, 2, 2)
    )
  )
})

test_that("can handle missing combinations", {
  sdf <- copy_to(
    sc,
    tibble::tribble(
      ~id, ~x_1, ~x_2, ~y_2,
      "A",    1,    2,  "a",
      "B",    3,    4,  "b",
    )
  )
  pv <- tidyr::pivot_longer(
    sdf, -id, names_to = c(".value", "n"), names_sep = "_"
  ) %>%
    collect()

  expect_equivalent(
    pv,
    tibble::tribble(
      ~id,  ~n, ~x,  ~y,
      "A", "1",  1,  NA,
      "A", "2",  2, "a",
      "B", "1",  3,  NA,
      "B", "2",  4, "b",
    )
  )
})
