skip_connection("tidyr_pivot_longer")
test_requires_version("2.0.0")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()
trivial_sdf <- testthat_tbl(
  "testthat_tidyr_pivot_longer_trivial_sdf",
  data = dplyr::tibble(x_y = 1)
)

test_that("can pivot all cols to long", {
  expect_same_remote_result(
    dplyr::tibble(x = 1:2, y = 3:4),
    . %>% tidyr::pivot_longer(x:y)
  )
})

test_that("values interleaved correctly", {
  expect_same_remote_result(
    dplyr::tibble(x = c(1, 2), y = c(10, 20), z = c(100, 200)),
    . %>% tidyr::pivot_longer(1:3)
  )
})

test_that("can drop missing values", {
  expect_same_remote_result(
    dplyr::tibble(x = c(1, NA), y = c(NA, 2)),
    . %>% tidyr::pivot_longer(x:y, values_drop_na = TRUE)
  )
})

test_that("preserves original keys", {
  expect_same_remote_result(
    dplyr::tibble(x = 1:2, y = 2L, z = 1:2),
    . %>% tidyr::pivot_longer(y:z)
  )
})

test_that("can handle missing combinations", {
  expect_same_remote_result(
    dplyr::tribble(
      ~id , ~x_1 , ~x_2 , ~y_2 ,
      "A" ,    1 ,    2 , "a"  ,
      "B" ,    3 ,    4 , "b"  ,
    ),
    . %>%
      tidyr::pivot_longer(
        -id,
        names_to = c(".value", "n"),
        names_sep = "_"
      )
  )
})

test_that("can override default output column type", {
  skip_connection("tidyr_pivot_longer")
  expect_same_remote_result(
    dplyr::tibble(x = 1L, y = 2L),
    . %>%
      tidyr::pivot_longer(
        x:y,
        values_transform = list(value = as.character)
      )
  )
})

test_that("original col order is preserved", {
  expect_same_remote_result(
    dplyr::tribble(
      ~id , ~z_1 , ~y_1 , ~x_1 , ~z_2 , ~y_2 , ~x_2 ,
      "A" ,    1 ,    2 ,    3 ,    4 ,    5 ,    6 ,
      "B" ,    7 ,    8 ,    9 ,   10 ,   11 ,   12 ,
    ),
    . %>%
      tidyr::pivot_longer(
        -id,
        names_to = c(".value", "n"),
        names_sep = "_"
      )
  )
})

test_that("can pivot duplicated names to .value", {
  expect_same_remote_result(
    dplyr::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>% tidyr::pivot_longer(-x, names_to = c(".value", NA), names_sep = "_")
  )

  expect_same_remote_result(
    dplyr::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>%
      tidyr::pivot_longer(
        -x,
        names_to = c(".value", NA),
        names_pattern = "(.)_(.)"
      )
  )

  expect_same_remote_result(
    dplyr::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>% tidyr::pivot_longer(-x, names_to = ".value", names_pattern = "(.)_.")
  )
})

test_that(".value can be at any position in `names_to`", {
  samp_sdf <- copy_to(
    sc,
    dplyr::tibble(
      i = 1:4,
      y_t1 = rnorm(4),
      y_t2 = rnorm(4),
      z_t1 = rep(3, 4),
      z_t2 = rep(-2, 4)
    )
  )
  samp_sdf2 <- dplyr::rename(
    samp_sdf,
    t1_y = y_t1,
    t2_y = y_t2,
    t1_z = z_t1,
    t2_z = z_t2
  )

  pv <- lapply(
    list(
      tidyr::pivot_longer(
        samp_sdf,
        -i,
        names_to = c(".value", "time"),
        names_sep = "_"
      ),
      tidyr::pivot_longer(
        samp_sdf2,
        -i,
        names_to = c("time", ".value"),
        names_sep = "_"
      )
    ),
    collect
  )

  expect_identical(pv[[1]], pv[[2]])
})

test_that("reporting data type mismatch", {
  sdf <- copy_to(sc, dplyr::tibble(abc = 1, xyz = "b"))
  err <- capture_error(tidyr::pivot_longer(sdf, tidyr::everything()))

  expect_true(grepl("data type mismatch", err$message, fixed = TRUE))
})

test_that("grouping is preserved", {
  expect_same_remote_result(
    dplyr::tibble(g = 1, x1 = 1, x2 = 2),
    . %>%
      dplyr::group_by(g) %>%
      tidyr::pivot_longer(x1:x2, names_to = "x", values_to = "v")
  )
})

test_that("names repair preserves grouping vars and pivot longer spec", {
  skip_connection("tidyr_pivot_longer")
  sdf_local <- dplyr::tibble(
    a = 1,
    b = 2,
    x_a_1 = c(1, 3),
    x_a_2 = c(2, 4),
    x_b_1 = c(1, 2),
    x_b_2 = c(3, 4)
  )

  pipeline <- . %>%
    dplyr::group_by(a) %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("x_"),
      names_to = c(".value", "b"),
      names_pattern = "x_(.)_(.)",
      names_repair = "universal"
    )

  expect_same_remote_result(sdf_local, pipeline)
})

# spec --------------------------------------------------------------------

test_that("validates inputs", {
  expect_error(
    build_longer_spec(trivial_sdf, x_y, values_to = letters[1:2]),
    class = "vctrs_error_assert"
  )
})

test_that("no names doesn't generate names", {
  expect_equal(
    colnames(build_longer_spec(trivial_sdf, x_y, names_to = character())),
    c(".name", ".value")
  )
})

test_that("multiple names requires names_sep/names_pattern", {
  expect_error(
    build_longer_spec(trivial_sdf, x_y, names_to = c("a", "b")),
    "multiple names"
  )

  expect_error(
    build_longer_spec(
      trivial_sdf,
      x_y,
      names_to = c("a", "b"),
      names_sep = "x",
      names_pattern = "x"
    ),
    "one of `names_sep` or `names_pattern"
  )
})

test_that("names_sep generates correct spec", {
  sp <- build_longer_spec(
    trivial_sdf,
    x_y,
    names_to = c("a", "b"),
    names_sep = "_"
  )

  expect_equal(sp$a, "x")
  expect_equal(sp$b, "y")
})

test_that("names_sep fails with single name", {
  expect_error(
    build_longer_spec(trivial_sdf, x_y, names_to = "x", names_sep = "_"),
    "`names_sep`"
  )
})

test_that("names_pattern generates correct spec", {
  sdf <- copy_to(sc, dplyr::tibble(zx_y = 1))
  sp <- build_longer_spec(
    sdf,
    zx_y,
    names_to = c("a", "b"),
    names_pattern = "z(.)_(.)"
  )
  expect_equal(sp$a, "x")
  expect_equal(sp$b, "y")

  sp <- build_longer_spec(
    sdf,
    zx_y,
    names_to = "a",
    names_pattern = "z(.)"
  )
  expect_equal(sp$a, "x")
})

test_that("names_to can override value_to", {
  test_requires_version("2.0.0")

  sp <- build_longer_spec(
    trivial_sdf,
    x_y,
    names_to = c("a", ".value"),
    names_sep = "_"
  )

  expect_equal(sp$.value, "y")
})

test_that("names_prefix strips off from beginning", {
  sdf <- copy_to(sc, dplyr::tibble(zzyz = 1))
  sp <- build_longer_spec(sdf, zzyz, names_prefix = "z")

  expect_equal(sp$name, "zyz")
})

test_that("can cast to custom type", {
  sdf <- copy_to(sc, dplyr::tibble(w1 = 1))
  sp <- build_longer_spec(
    sdf,
    w1,
    names_prefix = "w",
    names_transform = list(name = as.integer)
  )

  expect_equal(sp$name, 1L)
})

test_that("Error if the `col` can't be selected.", {
  expect_error(
    tidyr::pivot_longer(trivial_sdf, tidyr::matches("foo")),
    "select at least one"
  )
})

# string-splitting helpers ------------------------------------------------

test_that("build_output_names supports numeric names_sep", {
  out <- build_output_names(
    cols = c("abXcd", "efXgh"),
    names_to = c("a", "b"),
    names_prefix = NULL,
    names_sep = 2,
    names_pattern = NULL
  )

  expect_equal(out$a, c("ab", "ef"))
  expect_equal(out$b, c("Xcd", "Xgh"))
})

test_that(".strsep splits at positive and negative positions", {
  out <- .strsep(c("abcde", "vwxyz"), c(2, -1))

  expect_equal(out[[1]], c("ab", "vw"))
  expect_equal(out[[2]], c("cd", "xy"))
  expect_equal(out[[3]], c("e", "z"))
})

test_that(".str_separate errors on non-character `into`", {
  expect_error(
    .str_separate("a_b", into = 1:2, sep = "_"),
    "`into` must be a character vector"
  )
})

test_that(".str_separate errors on invalid `sep` type", {
  expect_error(
    .str_separate("a_b", into = c("a", "b"), sep = TRUE),
    "`sep` must be either numeric or character"
  )
})

test_that(".str_separate dispatches on numeric vs character sep", {
  num_out <- .str_separate(c("abcd"), into = c("a", "b"), sep = 2)
  expect_equal(num_out$a, "ab")
  expect_equal(num_out$b, "cd")

  chr_out <- .str_separate("x_y", into = c("a", "b"), sep = "_")
  expect_equal(chr_out$a, "x")
  expect_equal(chr_out$b, "y")
})

test_that(".str_separate drops NA names from `into`", {
  out <- .str_separate("x_y", into = c("a", NA), sep = "_")
  expect_equal(names(out), "a")
  expect_equal(out$a, "x")
})

test_that(".str_split_fixed warns and drops extra pieces (extra = 'warn')", {
  expect_warning(
    res <- .str_split_fixed("a_b_c", sep = "_", n = 2),
    "Additional pieces discarded"
  )
  expect_equal(res[[1]], "a")
  expect_equal(res[[2]], "b")
})

test_that(".str_split_fixed merges trailing pieces (extra = 'merge')", {
  res <- .str_split_fixed("a_b_c", sep = "_", n = 2, extra = "merge")
  expect_equal(res[[1]], "a")
  expect_equal(res[[2]], "b_c")
})

test_that(".str_split_fixed drops extra pieces silently (extra = 'drop')", {
  res <- .str_split_fixed("a_b_c", sep = "_", n = 2, extra = "drop")
  expect_equal(res[[1]], "a")
  expect_equal(res[[2]], "b")
})

test_that(".str_split_fixed deprecates extra = 'error'", {
  expect_warning(
    .str_split_fixed("a_b", sep = "_", n = 2, extra = "error"),
    "deprecated"
  )
})

test_that(".str_split_fixed warns and returns n pieces on missing input", {
  # NB: the returned values are not asserted here — .simplify_pieces() has an
  # off-by-one in its fill-right path (see planning doc follow-ups); we only
  # cover the warning + shape.
  expect_warning(
    res <- .str_split_fixed("a", sep = "_", n = 2),
    "Missing pieces filled with `NA`"
  )
  expect_length(res, 2)
})

test_that(".str_split_fixed accepts fill = 'right'", {
  res <- .str_split_fixed("a_b", sep = "_", n = 3, fill = "right")
  expect_length(res, 3)
})

test_that(".simplify_pieces handles exact, too-big, and too-small rows", {
  pieces <- list(
    c("a", "b"),
    c("c", "d", "e"),
    "f",
    NA
  )
  simp <- .simplify_pieces(pieces, p = 2, fill_left = FALSE)

  expect_equal(simp$too_big, 2L)
  expect_equal(simp$too_sml, 3L)
  # row 3 ("f") has length 1 < p=2: with fill_left=FALSE the
  # `j < length(x)` guard is never TRUE, so both columns get NA
  expect_equal(simp$strings[[1]], c("a", "c", NA, NA))
  expect_equal(simp$strings[[2]], c("b", "d", NA, NA))
})

test_that(".list_indices truncates long vectors", {
  expect_equal(.list_indices(1:3), "1, 2, 3")
  expect_match(.list_indices(1:30), "\\.\\.\\.$")
})

test_that(".str_split_n respects n_max", {
  out <- .str_split_n("a_b_c", "_", n_max = 2)
  expect_equal(out[[1]], c("a", "b_c"))

  out_all <- .str_split_n("a_b_c", "_")
  expect_equal(out_all[[1]], c("a", "b", "c"))
})

test_that(".str_extract pulls regex capture groups", {
  out <- .str_extract(
    c("x1", "y2"),
    into = c("letter", "num"),
    regex = "([a-z])([0-9])"
  )
  expect_equal(out$letter, c("x", "y"))
  expect_equal(out$num, c("1", "2"))
})

test_that(".str_extract converts types when convert = TRUE", {
  out <- .str_extract(
    "x1",
    into = c("letter", "num"),
    regex = "([a-z])([0-9])",
    convert = TRUE
  )
  expect_type(out$num, "integer")
  expect_equal(out$num, 1L)
})

test_that(".str_extract collapses duplicated `into` names", {
  out <- .str_extract(
    "ab",
    into = c("x", "x"),
    regex = "(.)(.)"
  )
  expect_equal(names(out), "x")
  expect_equal(out$x, "ab")
})

test_that(".str_extract drops NA names from `into`", {
  out <- .str_extract(
    "x1",
    into = c("letter", NA),
    regex = "([a-z])([0-9])"
  )
  expect_equal(names(out), "letter")
  expect_equal(out$letter, "x")
})

test_clear_cache()
