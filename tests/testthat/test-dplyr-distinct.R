skip_connection("dplyr-distinct")
skip_on_livy()

sc <- testthat_spark_connection()

test_that("distinct equivalent to local unique when keeping all columns", {
  df <- dplyr::tibble(
    x = c(2, 1, 2, 1, 1, 1, 1, 1, 2),
    y = c(2, 2, 1, 1, 1, 2, 1, 2, 2),
    z = c(2, 2, 1, 1, 2, 1, 1, 2, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))

  expect_equivalent(
    sdf %>% dplyr::distinct() %>% arrange(x, y, z) %>% collect(),
     df %>% dplyr::distinct() %>% arrange(x, y, z)
    )
})

test_that("distinct for single column works as expected", {
  df <- dplyr::tibble(
    x = c(1, 1, 1, 1),
    y = c(1, 1, 2, 2),
    z = c(1, 2, 1, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))
  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = FALSE) %>% arrange(x) %>% collect(), unique(df[order(df$x),"x"])
  )
  expect_equivalent(
    sdf %>% dplyr::distinct(y, .keep_all = FALSE) %>% arrange(y) %>% collect(), unique(df[order(df$y), "y"])
  )
})

test_that("distinct keeps only specified cols", {
  expect_equivalent(
    copy_to(sc, dplyr::tibble(x = c(1, 1, 1), y = c(1, 1, 1))) %>%
      dplyr::distinct(x) %>%
      collect(),
    dplyr::tibble(x = 1)
  )
})

test_that("unless .keep_all = TRUE", {

  df <- dplyr::tibble(x = c(1, 1, 1), y = 3:1)

  sdf <- copy_to(sc, df, name = random_string("tmp"))

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% collect(),
     df %>% dplyr::distinct(x)
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = TRUE) %>% collect(),
     df %>% dplyr::distinct(x, .keep_all = TRUE),
  )
})

test_that("distinct doesn't duplicate columns", {
  df <- dplyr::tibble(a = 1:3, b = 4:6)
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>% dplyr::distinct(a, a) %>% arrange(a) %>%  collect(),
     df %>% dplyr::distinct(a, a) %>% arrange(a)
  )
  expect_equivalent(
    sdf %>% dplyr::group_by(a) %>% dplyr::distinct(a) %>% arrange(a) %>% collect(),
     df %>% dplyr::group_by(a) %>% dplyr::distinct(a) %>% arrange(a)
  )
})

test_that("grouped distinct always includes group cols", {
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))
  out <- sdf %>%
    group_by(g) %>%
    distinct(x)

  expect_equivalent(out %>% arrange(g) %>% collect(), dplyr::tibble(g = c(1, 2), x = c(1, 2)))
  expect_equal(dplyr::group_vars(out), "g")
})

test_that("empty grouped distinct equivalent to empty ungrouped", {
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))

  df1 <- sdf %>%
    distinct() %>%
    group_by(g) %>%
    collect()
  df2 <- sdf %>%
    group_by(g) %>%
    distinct() %>%
    collect()

  expect_equal(df1, df2)
})

test_that("distinct on a new, mutated variable is equivalent to mutate followed by distinct", {
  df <- dplyr::tibble(g = c(1, 2), x = c(1, 2))
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g * 2) %>% arrange(aa) %>% collect(),
    df %>% dplyr::distinct(aa = g * 2) %>% arrange(aa)
  )
})

test_that("distinct on a new, copied variable is equivalent to mutate followed by distinct", {
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g) %>% arrange(aa) %>% collect(), dplyr::tibble(aa = c(1, 2))
  )
})

test_that("distinct preserves grouping", {
  df1 <- dplyr::tibble(x = c(1, 1, 2, 2), y = x)
  sdf1 <- copy_to(sc, df1, name = "distinct_df1")

  df <- df1 %>% dplyr::group_by(x)
  sdf <- sdf1 %>% dplyr::group_by(x)

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% arrange(x) %>% collect(),
     df %>% dplyr::distinct(x) %>% arrange(x)
    )

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% dplyr::group_vars(),
    df %>% dplyr::group_vars()
  )

  out <- sdf %>% dplyr::distinct(x = x + 2)


  expect_equivalent(
    sdf %>% dplyr::distinct(x = x + 2) %>% arrange(x) %>% collect(),
     df %>% dplyr::distinct(x = x + 2) %>% arrange(x)
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x = x + 2) %>% dplyr::group_vars(),
     df %>% dplyr::distinct(x = x + 2) %>% dplyr::group_vars()
  )

})

test_that("distinct followed by another lazy op works as expected", {
  sdf <- copy_to(
    sc,
    dplyr::tibble(
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
    dplyr::tibble(
      x = 1,
      y = c(1, 1, 2, 2),
      z = c(1, 2, 1, 2),
      r = 1
    )
  )
})

test_clear_cache()
