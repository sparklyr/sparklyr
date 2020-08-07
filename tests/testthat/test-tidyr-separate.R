context("tidyr-separate")

sc <- testthat_spark_connection()
simple_sdf <- testthat_tbl(
  name = "tidyr_separate_simple",
  data = tibble::tibble(x = "a:b")
)
sep_with_ints_sdf <- testthat_tbl(
  name = "tidyr_separate_with_int_vals",
  tibble::tibble(x = c(NA, "ab", "cd"))
)
len_mismatch_sdf <- testthat_tbl(
  name = "tidyr_length_mismatch",
  tibble::tibble(x = c("a b", "a b c"))
)

test_that("missing values in input are missing in output", {
  test_requires_version("3.0.0")

  sdf <- copy_to(sc, tibble::tibble(x = c(NA, "a b")))

  expect_equivalent(
    suppressWarnings(sdf %>% tidyr::separate(x, c("x", "y")) %>% collect()),
    tibble::tibble(x = c(NA, "a"), y = c(NA, "b"))
  )
})

test_that("positive integer values specific position between characters", {
  test_requires_version("3.0.0")

  expect_equivalent(
    sep_with_ints_sdf %>% tidyr::separate(x, c("x", "y"), 1) %>% collect(),
    tibble::tibble(x = c(NA, "a", "c"), y = c(NA, "b", "d"))
  )
})

test_that("negative integer values specific position between characters", {
  test_requires_version("3.0.0")

  expect_equivalent(
    sep_with_ints_sdf %>% tidyr::separate(x, c("x", "y"), -1) %>% collect(),
    tibble::tibble(x = c(NA, "a", "c"), y = c(NA, "b", "d"))
  )
})

test_that("extreme integer values handled sensibly", {
  test_requires_version("3.0.0")

  sdf <- copy_to(sc, tibble::tibble(x = c(NA, "a", "bc", "def")))
  expect_equivalent(
    sdf %>% tidyr::separate(x, c("x", "y"), 3) %>% collect(),
    tibble::tibble(x = c(NA, "a", "bc", "def"), y = c(NA, "", "", ""))
  )
  expect_equivalent(
    sdf %>% tidyr::separate(x, c("x", "y"), -3) %>% collect(),
    tibble::tibble(x = c(NA, "", "", ""), y = c(NA, "a", "bc", "def"))
  )
})

test_that("too many pieces dealt with as requested", {
  test_requires_version("3.0.0")

  suppressWarnings(
    expect_warning(
      tidyr::separate(len_mismatch_sdf, x, c("x", "y")),
      "Expected 2 piece\\(s\\)\\. Additional piece\\(s\\) discarded in 1 row\\(s\\) \\[2\\]\\."
    )
  )

  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "merge") %>% collect(),
    tibble::tibble(x = c("a", "a"), y = c("b", "b c"))
  )
  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "drop") %>% collect(),
    tibble::tibble(x = c("a", "a"), y = c("b", "b"))
  )
  suppressWarnings(expect_warning(tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "error"), "deprecated"))
})

test_that("too few pieces dealt with as requested", {
  test_requires_version("3.0.0")

  suppressWarnings(
    expect_warning(
      tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z")),
      "Expected 3 piece\\(s\\)\\. Missing piece\\(s\\) filled with NULL value\\(s\\) in 1 row\\(s\\) \\[1\\]\\."
    )
  )

  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z"), fill = "left") %>% collect(),
    tibble::tibble(x = c(NA, "a"), y = c("a", "b"), z = c("b", "c"))
  )
  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z"), fill = "right") %>% collect(),
    tibble::tibble(x = c("a", "a"), y = c("b", "b"), z = c(NA, "c"))
  )
})

test_that("preserves grouping", {
  test_requires_version("3.0.0")

  sdf <- simple_sdf %>% dplyr::mutate(g = 1) %>% dplyr::group_by(g)
  rs <- sdf %>% tidyr::separate(x, c("a", "b"))
  expect_equal(class(sdf), class(rs))
  expect_equal(dplyr::group_vars(sdf), dplyr::group_vars(rs))
})

test_that("drops grouping when needed", {
  test_requires_version("3.0.0")

  sdf <- simple_sdf %>% dplyr::group_by(x)
  rs <- sdf %>% tidyr::separate(x, c("a", "b"))
  expect_equivalent(rs %>% collect(), tibble::tibble(a = "a", b = "b"))
  expect_equal(dplyr::group_vars(rs), character())
})

test_that("overwrites existing columns", {
  test_requires_version("3.0.0")

  expect_equivalent(
    simple_sdf %>% tidyr::separate(x, c("x", "y")) %>% collect(),
    tibble::tibble(x = "a", y = "b")
  )
})

test_that("checks type of `into` and `sep`", {
  test_requires_version("3.0.0")

  expect_error(
    tidyr::separate(simple_sdf, x, "x", FALSE),
    "must be either numeric or character"
  )
  expect_error(
    tidyr::separate(simple_sdf, x, FALSE),
    "must be a character vector"
  )
})
