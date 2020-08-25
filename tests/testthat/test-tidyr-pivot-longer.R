context("tidyr-pivot-longer")

sc <- testthat_spark_connection()
trivial_sdf <- testthat_tbl(
  "testthat_tidyr_pivot_longer_trivial_sdf",
  data = tibble::tibble(x_y = 1)
)

test_that("can pivot all cols to long", {
  test_requires_version("2.0.0")

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
  test_requires_version("2.0.0")

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
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, tibble::tibble(x = c(1, NA), y = c(NA, 2)))
  pv <- tidyr::pivot_longer(sdf, x:y, values_drop_na = TRUE) %>% collect()

  expect_equivalent(pv, tibble::tibble(name = c("x", "y"), value = c(1, 2)))
})

test_that("preserves original keys", {
  test_requires_version("2.0.0")

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
  test_requires_version("2.0.0")

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

test_that("can override default output column type", {
  test_requires_version("2.0.0")

  sdf <- copy_to(sc, tibble::tibble(x = 1L, y = 2L))
  pv <- tidyr::pivot_longer(
    sdf, x:y, values_transform = list(value = as.character)
  ) %>%
    collect()

  expect_equivalent(
    pv,
    tibble::tibble(name = c("x", "y"), value = c("1", "2"))
  )
})

test_that("original col order is preserved", {
  test_requires_version("2.0.0")

  sdf <- copy_to(
    sc,
    tibble::tribble(
      ~id, ~z_1, ~y_1, ~x_1, ~z_2,  ~y_2, ~x_2,
      "A",    1,    2,    3,     4,    5,    6,
      "B",    7,    8,    9,    10,   11,   12,
    )
  )
  pv <- tidyr::pivot_longer(
    sdf, -id, names_to = c(".value", "n"), names_sep = "_"
  ) %>%
    collect()

  expect_equivalent(
    pv,
    tibble::tribble(
      ~id,  ~n, ~z, ~y, ~x,
      "A", "1",  1,  2,  3,
      "A", "2",  4,  5,  6,
      "B", "1",  7,  8,  9,
      "B", "2", 10, 11, 12,
    )
  )
})

test_that("can pivot duplicated names to .value", {
  sdf <- copy_to(sc, tibble::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4))
  pv <- lapply(
    list(
      tidyr::pivot_longer(sdf, -x, names_to = c(".value", NA), names_sep = "_"),
      tidyr::pivot_longer(sdf, -x, names_to = c(".value", NA), names_pattern = "(.)_(.)"),
      tidyr::pivot_longer(sdf, -x, names_to = ".value", names_pattern = "(.)_.")
    ),
    collect
  )
  for (x in pv) {
    expect_equivalent(x, tibble::tibble(x = 1, a = 1:2, b = 3:4))
  }
})

test_that(".value can be at any position in `names_to`", {
  samp_sdf <- copy_to(
    sc,
    tibble::tibble(
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
        samp_sdf, -i, names_to = c(".value", "time"), names_sep = "_"
      ),
      tidyr::pivot_longer(
        samp_sdf2, -i, names_to = c("time", ".value"), names_sep = "_"
      )
    ),
    collect
  )

  expect_identical(pv[[1]], pv[[2]])
})

test_that("reporting data type mismatch", {
  sdf <- copy_to(sc, tibble::tibble(abc = 1, xyz = "b"))
  err <- capture_error(tidyr::pivot_longer(sdf, tidyr::everything()))

  expect_true(grepl("data type mismatch", err$message, fixed = TRUE))
})

test_that("grouping is preserved", {
  sdf <- copy_to(sc, tibble::tibble(g = 1, x1 = 1, x2 = 2))
  out <- sdf %>%
    dplyr::group_by(g) %>%
    tidyr::pivot_longer(x1:x2, names_to = "x", values_to = "v")

  expect_equal(dplyr::group_vars(out), "g")
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
    build_longer_spec(trivial_sdf, x_y,
      names_to = c("a", "b"),
      names_sep = "x",
      names_pattern = "x"
    ),
    "one of `names_sep` or `names_pattern"
  )
})

test_that("names_sep generates correct spec", {
  sp <- build_longer_spec(
    trivial_sdf, x_y, names_to = c("a", "b"), names_sep = "_"
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
  sdf <- copy_to(sc, tibble::tibble(zx_y = 1))
  sp <- build_longer_spec(
    sdf, zx_y, names_to = c("a", "b"), names_pattern = "z(.)_(.)"
  )
  expect_equal(sp$a, "x")
  expect_equal(sp$b, "y")

  sp <- build_longer_spec(
    sdf, zx_y, names_to = "a", names_pattern = "z(.)"
  )
  expect_equal(sp$a, "x")
})

test_that("names_to can override value_to", {
  sp <- build_longer_spec(
    trivial_sdf, x_y, names_to = c("a", ".value"), names_sep = "_"
  )

  expect_equal(sp$.value, "y")
})

test_that("names_prefix strips off from beginning", {
  sdf <- copy_to(sc, tibble::tibble(zzyz = 1))
  sp <- build_longer_spec(sdf, zzyz, names_prefix = "z")

  expect_equal(sp$name, "zyz")
})

test_that("can cast to custom type", {
  sdf <- copy_to(sc, tibble::tibble(w1 = 1))
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
