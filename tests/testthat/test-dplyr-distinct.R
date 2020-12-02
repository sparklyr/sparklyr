context("dplyr distinct")

sc <- testthat_spark_connection()

test_that("distinct equivalent to local unique when keeping all columns", {
  df <- tibble::tibble(
    x = c(2, 1, 2, 1, 1, 1, 1, 1, 2),
    y = c(2, 2, 1, 1, 1, 2, 1, 2, 2),
    z = c(2, 2, 1, 1, 2, 1, 1, 2, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))

  expect_equivalent(sdf %>% dplyr::distinct() %>% collect(), unique(df))
})

test_that("distinct for single column works as expected", {
  df <- tibble::tibble(
    x = c(1, 1, 1, 1),
    y = c(1, 1, 2, 2),
    z = c(1, 2, 1, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))
  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = FALSE) %>% collect(), unique(df["x"])
  )
  expect_equivalent(
    sdf %>% dplyr::distinct(y, .keep_all = FALSE) %>% collect(), unique(df["y"])
  )
})

test_that("distinct keeps only specified cols", {
  expect_equivalent(
    copy_to(sc, tibble::tibble(x = c(1, 1, 1), y = c(1, 1, 1))) %>%
      dplyr::distinct(x) %>%
      collect(),
    tibble::tibble(x = 1)
  )
})

test_that("unless .keep_all = TRUE", {
  sdf <- copy_to(
    sc,
    tibble::tibble(x = c(1, 1, 1), y = 3:1),
    name = random_string("tmp")
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% collect(), tibble::tibble(x = 1)
  )
  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = TRUE) %>% collect(),
    tibble::tibble(x = 1, y = 3L)
  )

})

test_that("distinct doesn't duplicate columns", {
  sdf <- copy_to(sc, tibble::tibble(a = 1:3, b = 4:6))

  expect_equivalent(
    sdf %>% dplyr::distinct(a, a) %>% collect(),
    tibble::tibble(a = 1:3)
  )
  expect_equivalent(
    sdf %>% dplyr::group_by(a) %>% dplyr::distinct(a) %>% collect(),
    tibble::tibble(a = 1:3)
  )
})

test_that("grouped distinct always includes group cols", {
  sdf <- copy_to(sc, tibble::tibble(g = c(1, 2), x = c(1, 2)))
  out <- sdf %>% group_by(g) %>% distinct(x)

  expect_equivalent(out %>% collect(), tibble::tibble(g = c(1, 2), x = c(1, 2)))
  expect_equal(dplyr::group_vars(out), "g")
})

test_that("empty grouped distinct equivalent to empty ungrouped", {
  sdf <- copy_to(sc, tibble::tibble(g = c(1, 2), x = c(1, 2)))

  df1 <- sdf %>% distinct() %>% group_by(g) %>% collect()
  df2 <- sdf %>% group_by(g) %>% distinct() %>% collect()

  expect_equal(df1, df2)
})

test_that("distinct on a new, mutated variable is equivalent to mutate followed by distinct", {
  sdf <- copy_to(sc, tibble::tibble(g = c(1, 2), x = c(1, 2)))

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g * 2) %>% collect(), tibble::tibble(aa = c(2, 4))
  )
})

test_that("distinct on a new, copied variable is equivalent to mutate followed by distinct", {
  sdf <- copy_to(sc, tibble::tibble(g = c(1, 2), x = c(1, 2)))

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g) %>% collect(), tibble::tibble(aa = c(1, 2))
  )
})

test_that("distinct preserves grouping", {
  sdf <- copy_to(sc, tibble::tibble(x = c(1, 1, 2, 2), y = x)) %>%
    dplyr::group_by(x)

  out <- sdf %>% dplyr::distinct(x)
  expect_equivalent(out %>% collect(), tibble::tibble(x = c(1, 2)))
  expect_equal(out %>% dplyr::group_vars(), "x")

  out <- sdf %>% dplyr::distinct(x = x + 2)
  expect_equivalent(out %>% collect(), tibble::tibble(x = c(3, 4)))
  expect_equal(out %>% dplyr::group_vars(), "x")
})

test_that("distinct followed by another lazy op works as expected", {
  sdf <- copy_to(
    sc,
    tibble::tibble(
      x = 1,
      y = c(1, 1, 2, 2, 1),
      z = c(1, 2, 1, 2, 1)
    )
  )

  expect_equivalent(
    sdf %>%
      dplyr::distinct() %>%
      dplyr::mutate(r = 1) %>%
      dplyr::arrange(y, z) %>%
      collect(),
    tibble::tibble(
      x = 1,
      y = c(1, 1, 2, 2),
      z = c(1, 2, 1, 2),
      r = 1
    )
  )
})
