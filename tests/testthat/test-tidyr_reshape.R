skip_connection("tidyr_reshape")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()
simple_sdf <- testthat_tbl(
  name = "tidyr_separate_simple",
  data = dplyr::tibble(x = "a:b", id = 1L)
)
sep_with_ints_sdf <- testthat_tbl(
  name = "tidyr_separate_with_int_vals",
  dplyr::tibble(x = c(NA, "ab", "cd"), id = 1:3)
)
len_mismatch_sdf <- testthat_tbl(
  name = "tidyr_length_mismatch",
  dplyr::tibble(x = c("a b", "a b c"), id = 1:2)
)

tidyr_unite_test_data <- dplyr::tibble(g = 1L, x = "a", y = "b")
sdf <- testthat_tbl("tidyr_unite_test_data")
tidyr_unite_na_test_data <- tidyr::expand_grid(x = c("a", NA), y = c("b", NA))
na_sdf <- testthat_tbl("tidyr_unite_na_test_data")

test_that("missing values in input are missing in output", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, dplyr::tibble(x = c(NA, "a b")))

  expect_equivalent(
    suppressWarnings(sdf %>% tidyr::separate(x, c("x", "y")) %>% collect()),
    dplyr::tibble(x = c(NA, "a"), y = c(NA, "b"))
  )
})

test_that("positive integer values specific position between characters", {
  test_requires_version("2.4.0")

  expect_equivalent(
    sep_with_ints_sdf %>% tidyr::separate(x, c("x", "y"), 1) %>% collect(),
    dplyr::tibble(id = 1:3, x = c(NA, "a", "c"), y = c(NA, "b", "d"))
  )
})

test_that("negative integer values specific position between characters", {
  test_requires_version("2.4.0")

  expect_equivalent(
    sep_with_ints_sdf %>% tidyr::separate(x, c("x", "y"), -1) %>% collect(),
    dplyr::tibble(id = 1:3, x = c(NA, "a", "c"), y = c(NA, "b", "d"))
  )
})

test_that("extreme integer values handled sensibly", {
  test_requires_version("2.4.0")

  sdf <- copy_to(sc, dplyr::tibble(id = 1:4, x = c(NA, "a", "bc", "def")))
  expect_equivalent(
    sdf %>% tidyr::separate(x, c("x", "y"), 3) %>% collect(),
    dplyr::tibble(id = 1:4, x = c(NA, "a", "bc", "def"), y = c(NA, "", "", ""))
  )
  expect_equivalent(
    sdf %>% tidyr::separate(x, c("x", "y"), -3) %>% collect(),
    dplyr::tibble(id = 1:4, x = c(NA, "", "", ""), y = c(NA, "a", "bc", "def"))
  )
})

test_that("too many pieces dealt with as requested", {
  test_requires_version("2.4.0")

  suppressWarnings(
    expect_warning(
      tidyr::separate(len_mismatch_sdf, x, c("x", "y")),
      "Expected 2 piece\\(s\\)\\. Additional piece\\(s\\) discarded in 1 row\\(s\\) \\[2\\]\\."
    )
  )

  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "merge") %>%
      collect(),
    dplyr::tibble(id = 1:2, x = c("a", "a"), y = c("b", "b c"))
  )
  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "drop") %>%
      collect(),
    dplyr::tibble(id = 1:2, x = c("a", "a"), y = c("b", "b"))
  )
  suppressWarnings(expect_warning(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y"), extra = "error"),
    "deprecated"
  ))
})

test_that("too few pieces dealt with as requested", {
  test_requires_version("2.4.0")

  suppressWarnings(
    expect_warning(
      tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z")),
      "Expected 3 piece\\(s\\)\\. Missing piece\\(s\\) filled with NULL value\\(s\\) in 1 row\\(s\\) \\[1\\]\\."
    )
  )

  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z"), fill = "left") %>%
      collect(),
    dplyr::tibble(id = 1:2, x = c(NA, "a"), y = c("a", "b"), z = c("b", "c"))
  )
  expect_equivalent(
    tidyr::separate(len_mismatch_sdf, x, c("x", "y", "z"), fill = "right") %>%
      collect(),
    dplyr::tibble(id = 1:2, x = c("a", "a"), y = c("b", "b"), z = c(NA, "c"))
  )
})

test_that("preserves grouping", {
  test_requires_version("2.4.0")

  sdf <- simple_sdf %>%
    dplyr::mutate(g = 1) %>%
    dplyr::group_by(g)
  rs <- sdf %>% tidyr::separate(x, c("a", "b"))
  expect_equal(class(sdf), class(rs))
  expect_equal(dplyr::group_vars(sdf), dplyr::group_vars(rs))
})

test_that("drops grouping when needed", {
  test_requires_version("2.4.0")

  sdf <- simple_sdf %>% dplyr::group_by(x)
  rs <- sdf %>% tidyr::separate(x, c("a", "b"))
  expect_equivalent(rs %>% collect(), dplyr::tibble(id = 1L, a = "a", b = "b"))
  expect_equal(dplyr::group_vars(rs), character())
})

test_that("overwrites existing columns", {
  test_requires_version("2.4.0")

  expect_equivalent(
    simple_sdf %>% tidyr::separate(x, c("x", "y")) %>% collect(),
    dplyr::tibble(id = 1, x = "a", y = "b")
  )
})

test_that("checks type of `into` and `sep`", {
  test_requires_version("2.4.0")

  expect_error(
    tidyr::separate(simple_sdf, x, "x", FALSE),
    "must be either numeric or character"
  )
  expect_error(
    tidyr::separate(simple_sdf, x, FALSE),
    "must be a character vector"
  )
})

test_that("unite pastes columns together & removes old col", {
  expect_equivalent(
    sdf %>% tidyr::unite(z, x:y) %>% collect(),
    dplyr::tibble(g = 1, z = "a_b")
  )
})

test_that("unite does not remove new col in case of name clash", {
  expect_equivalent(
    sdf %>% tidyr::unite(x, x:y) %>% collect(),
    dplyr::tibble(g = 1, x = "a_b")
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
  expect_equivalent(rs %>% collect(), dplyr::tibble(gx = "1_a", y = "b"))
  expect_equal(dplyr::group_vars(rs), character())
})

test_that("empty var spec uses all vars", {
  expect_equivalent(
    tidyr::unite(sdf, "z") %>% sdf_collect(),
    dplyr::tibble(z = "1_a_b")
  )
})

test_that("can specify separator", {
  expect_equivalent(
    tidyr::unite(sdf, "z", sep = "-") %>% sdf_collect(),
    dplyr::tibble(z = "1-a-b")
  )
})

test_that("can handle missing vars correctly when na.rm = FALSE", {
  expect_equivalent(
    na_sdf %>%
      tidyr::unite("z", x:y, na.rm = FALSE) %>%
      sdf_collect(),
    dplyr::tibble(z = c("a_b", "a_NA", "NA_b", "NA_NA"))
  )
})

test_that("can remove missing vars on request", {
  expect_equivalent(
    na_sdf %>%
      tidyr::unite("z", x:y, na.rm = TRUE) %>%
      sdf_collect(),
    dplyr::tibble(z = c("a_b", "a", "b", ""))
  )
})

test_that("regardless of the type of the NA", {
  na_sdf <- copy_to(
    sc,
    dplyr::tibble(
      x = c("x", "y", "z"),
      lgl = NA,
      dbl = NA_real_,
      chr = NA_character_
    ),
    overwrite = TRUE
  )

  vec_unite <- function(vars) {
    rs <- tidyr::unite(
      na_sdf,
      "out",
      tidyselect::any_of(vars),
      na.rm = TRUE
    ) %>%
      collect()

    rs$out
  }

  expect_equal(vec_unite(c("x", "lgl")), c("x", "y", "z"))
  expect_equal(vec_unite(c("x", "dbl")), c("x", "y", "z"))
  expect_equal(vec_unite(c("x", "chr")), c("x", "y", "z"))
})

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
  ) %>%
    dplyr::mutate(
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
  ) %>%
    dplyr::mutate(
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
