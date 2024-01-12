skip_connection("sdf-expand-grid")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

var1 <- seq(26)
var2 <- letters
var3 <- c(1.111, 3.3, 2.22222, NaN, 4.4444, 5.5)
var4 <- c("foo", NA, "bar", "", "baz", NA)

test_that("sdf_expand_grid works with R vectors", {
  test_requires_version("2.0.0")

  expect_equivalent(
    sdf_expand_grid(sc, var1, var2, var3, var4) %>% collect(),
    expand.grid(var1, var2, var3, var4, stringsAsFactors = FALSE)
  )

  expect_equivalent(
    sdf_expand_grid(sc, x = var1, var2, y = var3, var4) %>% collect(),
    expand.grid(x = var1, var2, y = var3, var4, stringsAsFactors = FALSE)
  )

  expect_equivalent(
    sdf_expand_grid(sc, x = var1, y = var2, z = var3, w = var4) %>% collect(),
    expand.grid(x = var1, y = var2, z = var3, w = var4, stringsAsFactors = FALSE)
  )
})

test_that("sdf_expand_grid works Spark dataframes", {
  test_requires_version("2.0.0")

  df1 <- dplyr::tibble(x = var1, y = var2)
  df2 <- dplyr::tibble(z = var3, w = var4)

  expect_equivalent(
    sdf_expand_grid(
      sc,
      copy_to(sc, df1, name = random_string("tmp")),
      copy_to(sc, df2, name = random_string("tmp"))
    ) %>%
      collect(),
    merge(df1, df2, all = TRUE)
  )
})

test_that("sdf_expand_grid works with a mixture of R vectors and Spark dataframes", {
  test_requires_version("2.0.0")

  df1 <- dplyr::tibble(y = var1, z = var2)

  expect_equivalent(
    sdf_expand_grid(
      sc,
      x = var3,
      copy_to(sc, df1, name = random_string("tmp")),
      var4
    ) %>%
      collect(),
    merge(dplyr::tibble(x = var3), df1, all = TRUE) %>%
      merge(dplyr::tibble(Var3 = var4), all = TRUE)
  )
})

test_that("sdf_expand_grid works with broadcast joins", {
  test_requires_version("2.0.0")

  df1 <- dplyr::tibble(y = var1, z = var2)
  sdf1 <- copy_to(sc, df1, name = random_string("tmp"))

  expect_equivalent(
    sdf_expand_grid(
      sc,
      x = var3,
      y = sdf1,
      var4,
      broadcast_vars = c(x, y)
    ) %>%
      collect() %>%
      dplyr::arrange(dplyr::pick(dplyr::everything())),
    merge(dplyr::tibble(x = var3), df1, all = TRUE) %>%
      merge(dplyr::tibble(Var3 = var4), all = TRUE) %>%
      dplyr::arrange(dplyr::pick(dplyr::everything()))
  )

  expect_equivalent(
    sdf_expand_grid(
      sc,
      x = var3,
      y = sdf1,
      var4,
      broadcast_vars = c("x", "y")
    ) %>%
      collect() %>%
      dplyr::arrange(x, y, z, Var3),
    merge(dplyr::tibble(x = var3), df1, all = TRUE) %>%
      merge(dplyr::tibble(Var3 = var4), all = TRUE) %>%
      dplyr::arrange(dplyr::pick(dplyr::everything()))
  )
})

test_clear_cache()

