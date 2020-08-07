context("tidyr-unite")

sc <- testthat_spark_connection()
tidyr_unite_test_data <- tibble::tibble(g = 1L, x = "a", y = "b")
sdf <- testthat_tbl("tidyr_unite_test_data")
tidyr_unite_na_test_data <- tidyr::expand_grid(x = c("a", NA), y = c("b", NA))
na_sdf <- testthat_tbl("tidyr_unite_na_test_data")

test_that("unite pastes columns together & removes old col", {
  expect_equivalent(
    sdf %>% tidyr::unite(z, x:y) %>% collect(),
    tibble::tibble(g = 1, z = "a_b")
  )
})

test_that("unite does not remove new col in case of name clash", {
  expect_equivalent(
    sdf %>% tidyr::unite(x, x:y) %>% collect(),
    tibble::tibble(g = 1, x = "a_b")
  )
})

test_that("unite preserves grouping", {
  sdf_g <- sdf %>% dplyr::group_by(g)
  rs <- sdf_g %>% tidyr::unite(x, x)
  expect_equivalent(rs %>% collect(), sdf %>% collect())
  expect_equal(dplyr::group_vars(sdf_g), dplyr::group_vars(rs))
})

test_that("drops grouping when needed", {
  sdf_g <- sdf %>% dplyr::group_by(g)
  rs <- sdf_g %>% tidyr::unite(gx, g, x)
  expect_equivalent(rs %>% collect(), tibble::tibble(gx = "1_a", y = "b"))
  expect_equal(dplyr::group_vars(rs), character())
})

test_that("empty var spec uses all vars", {
  expect_equivalent(
    tidyr::unite(sdf, "z") %>% sdf_collect(),
    tibble::tibble(z = "1_a_b")
  )
})

test_that("can specify separator", {
  expect_equivalent(
    tidyr::unite(sdf, "z", sep = "-") %>% sdf_collect(),
    tibble::tibble(z = "1-a-b")
  )
})

test_that("can handle missing vars correctly when na.rm = FALSE", {
  expect_equivalent(
    na_sdf %>%
      tidyr::unite("z", x:y, na.rm = FALSE) %>%
      sdf_collect(),
    tibble::tibble(z = c("a_b", "a_NA", "NA_b", "NA_NA"))
  )
})

test_that("can remove missing vars on request", {
  expect_equivalent(
    na_sdf %>%
      tidyr::unite("z", x:y, na.rm = TRUE) %>%
      sdf_collect(),
    tibble::tibble(z = c("a_b", "a", "b", ""))
  )
})

test_that("regardless of the type of the NA", {
  na_sdf <- copy_to(
    sc,
    tibble::tibble(
      x = c("x", "y", "z"),
      lgl = NA,
      dbl = NA_real_,
      chr = NA_character_
    ),
    overwrite = TRUE
  )

  vec_unite <- function(vars) {
    rs <- tidyr::unite(
      na_sdf, "out", tidyselect::any_of(vars),
      na.rm = TRUE
    ) %>%
      collect()

    rs$out
  }

  expect_equal(vec_unite(c("x", "lgl")), c("x", "y", "z"))
  expect_equal(vec_unite(c("x", "dbl")), c("x", "y", "z"))
  expect_equal(vec_unite(c("x", "chr")), c("x", "y", "z"))
})
