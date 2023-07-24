skip_connection("tidyr-pivot-longer")
test_requires_version("2.0.0")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()
trivial_sdf <- testthat_tbl(
  "testthat_tidyr_pivot_longer_trivial_sdf",
  data = tibble::tibble(x_y = 1)
)

test_that("can pivot all cols to long", {
  expect_same_remote_result(
    tibble::tibble(x = 1:2, y = 3:4),
    . %>% tidyr::pivot_longer(x:y)
    )
})

test_that("values interleaved correctly", {
  expect_same_remote_result(
    tibble::tibble(x = c(1, 2), y = c(10, 20), z = c(100, 200)),
    . %>% tidyr::pivot_longer(1:3)
  )
})

test_that("can drop missing values", {
  expect_same_remote_result(
    tibble::tibble(x = c(1, NA), y = c(NA, 2)),
    . %>% tidyr::pivot_longer(x:y, values_drop_na = TRUE)
  )
})

test_that("preserves original keys", {
  expect_same_remote_result(
    tibble::tibble(x = 1:2, y = 2L, z = 1:2),
    . %>% tidyr::pivot_longer(y:z)
  )
})

test_that("can handle missing combinations", {
  expect_same_remote_result(
    tibble::tribble(
      ~id, ~x_1, ~x_2, ~y_2,
      "A",    1,    2,  "a",
      "B",    3,    4,  "b",
    ),
    . %>% tidyr::pivot_longer(
        -id,
        names_to = c(".value", "n"), names_sep = "_"
      )
  )
})

test_that("can override default output column type", {
  expect_same_remote_result(
    tibble::tibble(x = 1L, y = 2L),
    . %>% tidyr::pivot_longer(
      x:y,
      values_transform = list(value = as.character)
    )
  )
})

test_that("original col order is preserved", {
  expect_same_remote_result(
    tibble::tribble(
      ~id, ~z_1, ~y_1, ~x_1, ~z_2, ~y_2, ~x_2,
      "A", 1, 2, 3, 4, 5, 6,
      "B", 7, 8, 9, 10, 11, 12,
    ),
    . %>% tidyr::pivot_longer(
      -id,
      names_to = c(".value", "n"), names_sep = "_"
    )
  )
})

test_that("can pivot duplicated names to .value", {
  expect_same_remote_result(
    tibble::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>% tidyr::pivot_longer(-x, names_to = c(".value", NA), names_sep = "_")
  )

  expect_same_remote_result(
    tibble::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>% tidyr::pivot_longer(-x, names_to = c(".value", NA), names_pattern = "(.)_(.)")
  )

  expect_same_remote_result(
    tibble::tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4),
    . %>% tidyr::pivot_longer(-x, names_to = ".value", names_pattern = "(.)_.")
  )
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
        samp_sdf, -i,
        names_to = c(".value", "time"), names_sep = "_"
      ),
      tidyr::pivot_longer(
        samp_sdf2, -i,
        names_to = c("time", ".value"), names_sep = "_"
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
  expect_same_remote_result(
    tibble::tibble(g = 1, x1 = 1, x2 = 2),
    . %>%
      dplyr::group_by(g) %>%
      tidyr::pivot_longer(x1:x2, names_to = "x", values_to = "v")
  )
})

test_that("names repair preserves grouping vars and pivot longer spec", {
  sdf_local <- tibble::tibble(
    a = 1, b = 2,
    x_a_1 = c(1, 3), x_a_2 = c(2, 4), x_b_1 = c(1, 2), x_b_2 = c(3, 4)
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
    trivial_sdf, x_y,
    names_to = c("a", "b"), names_sep = "_"
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
    sdf, zx_y,
    names_to = c("a", "b"), names_pattern = "z(.)_(.)"
  )
  expect_equal(sp$a, "x")
  expect_equal(sp$b, "y")

  sp <- build_longer_spec(
    sdf, zx_y,
    names_to = "a", names_pattern = "z(.)"
  )
  expect_equal(sp$a, "x")
})

test_that("names_to can override value_to", {
  test_requires_version("2.0.0")

  sp <- build_longer_spec(
    trivial_sdf, x_y,
    names_to = c("a", ".value"), names_sep = "_"
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
