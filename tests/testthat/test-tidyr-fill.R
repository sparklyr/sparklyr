skip_connection("tidyr-fill")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("all missings left unchanged", {
  test_requires_version("2.0.0")

  sdf <- copy_to(
    sc,
    dplyr::tibble(
      lgl = c(NA, NA),
      int = c(NA_integer_, NA),
      dbl = c(NA_real_, NA),
      chr = c(NA_character_, NA)
    )
  )

  down <- tidyr::fill(sdf, lgl, int, dbl, chr)
  up <- tidyr::fill(sdf, lgl, int, dbl, chr, .direction = "up")

  for (rs in list(down, up)) {
    for (col in colnames(sdf)) {
      expect_equivalent(
        rs %>%
          dplyr::mutate(is_na = is.na(!!rlang::sym(col))) %>%
          dplyr::select(is_na) %>%
          collect(),
        dplyr::tibble(is_na = c(TRUE, TRUE))
      )
    }
  }
})

test_that("missings are filled correctly", {
  test_requires_version("2.0.0")

  # filled down from last non-missing
  sdf <- copy_to(sc, dplyr::tibble(x = c(NA, 1, NA, 2, NA, NA)))

  out <- tidyr::fill(sdf, x) %>% collect()
  expect_equal(out$x, c(NA, 1, 1, 2, 2, 2))

  out <- tidyr::fill(sdf, x, .direction = "up") %>% collect()
  expect_equal(out$x, c(1, 1, 2, 2, NA, NA))

  out <- tidyr::fill(sdf, x, .direction = "downup") %>% collect()
  expect_equal(out$x, c(1, 1, 1, 2, 2, 2))

  out <- tidyr::fill(sdf, x, .direction = "updown") %>% collect()
  expect_equal(out$x, c(1, 1, 2, 2, 2, 2))
})

test_that("missings filled down for each atomic vector", {
  test_requires_version("2.0.0")
  skip_on_arrow()

  sdf <- copy_to(
    sc,
    dplyr::tibble(
      lgl = c(TRUE, NA),
      int = c(1L, NA),
      dbl = c(1, NA),
      chr = c("a", NA)
    )
  ) %>% dplyr::mutate(
    arr = dplyr::sql("IF(lgl, array(1, 2, 3, 4, 5), NULL)")
  )
  out <- sdf %>%
    tidyr::fill(tidyselect::everything()) %>%
    collect()

  expect_equal(out$lgl, c(TRUE, TRUE))
  expect_equal(out$int, c(1L, 1L))
  expect_equal(out$dbl, c(1, 1))
  expect_equal(out$chr, c("a", "a"))
  expect_equal(out$arr, list(1:5, 1:5))
})

test_that("missings filled up for each atomic vector", {
  test_requires_version("2.0.0")
  skip_on_arrow()

  sdf <- copy_to(
    sc,
    dplyr::tibble(
      lgl = c(NA, TRUE),
      int = c(NA, 1L),
      dbl = c(NA, 1),
      chr = c(NA, "a")
    )
  ) %>% dplyr::mutate(
    arr = dplyr::sql("IF(lgl, array(1, 2, 3, 4, 5), NULL)")
  )
  out <- sdf %>%
    tidyr::fill(tidyselect::everything(), .direction = "up") %>%
    collect()

  expect_equal(out$lgl, c(TRUE, TRUE))
  expect_equal(out$int, c(1L, 1L))
  expect_equal(out$dbl, c(1, 1))
  expect_equal(out$chr, c("a", "a"))
  expect_equal(out$arr, list(1:5, 1:5))
})

test_that("fill respects grouping", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, dplyr::tibble(x = c(1, 1, 2), y = c(1, NA, NA)))
  out <- sdf %>%
    dplyr::group_by(x) %>%
    tidyr::fill(y) %>%
    collect()
  expect_equal(out$y, c(1, 1, NA))
})

test_that("fill respects grouping", {
  test_requires_version("2.0.0")

  df <- dplyr::tibble(
    id1 = c(1, 4, 2, 8, 5, 7),
    id2 = c(4, 1, 7, 5, 8, 2),
    value = c(1, NA, 2, NA, 5, NA)
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>%
      dplyr::arrange(id1) %>%
      tidyr::fill(value, .direction = "down") %>%
      collect(),
    df %>%
      dplyr::arrange(id1) %>%
      tidyr::fill(value, .direction = "down")
  )

  expect_equivalent(
    sdf %>%
      dplyr::arrange(id2, id1) %>%
      tidyr::fill(value, .direction = "up") %>%
      collect(),
    df %>%
      dplyr::arrange(id2, id1) %>%
      tidyr::fill(value, .direction = "up")
  )

  expect_equivalent(
    sdf %>%
      dplyr::arrange(id1 * id2, id1 + id2) %>%
      tidyr::fill(value, .direction = "updown") %>%
      collect(),
    df %>%
      dplyr::arrange(id1 * id2, id1 + id2) %>%
      tidyr::fill(value, .direction = "updown")
  )

  expect_equivalent(
    sdf %>%
      dplyr::arrange(id1 + id2, id1 * id2) %>%
      tidyr::fill(value, .direction = "downup") %>%
      collect(),
    df %>%
      dplyr::arrange(id1 * id2, id1 + id2) %>%
      tidyr::fill(value, .direction = "downup")
  )
})

test_clear_cache()

