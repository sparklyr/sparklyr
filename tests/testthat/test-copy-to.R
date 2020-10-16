context("copy data")

sc <- testthat_spark_connection()

test_that("sdf_copy_to works for default serializer", {
  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble(.name_repair = "unique")
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("spark_table_name() doesn't warn for multiline expression (#1386)", {
  expect_warning(
    spark_table_name(data.frame(
      foo = c(1, 2, 3),
      bar = c(2, 1, 3),
      foobar = c("a", "b", "c")
    )),
    NA
  )
})

test_that("sdf_copy_to supports list of callbacks", {
  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble(.name_repair = "unique")
  df_tbl <- sdf_copy_to(sc, list(~df, ~df), overwrite = TRUE)

  expect_equal(
    sdf_nrow(df_tbl),
    4
  )
})

test_that("sdf_copy_to works for json serializer", {
  dfjson <- tibble::tibble(
    g = c(1, 2, 3),
    data = list(
      tibble::tibble(x = 1, y = 2),
      tibble::tibble(x = 4:5, y = 6:7),
      tibble::tibble(x = 10)
    )
  )

  dfjson_tbl <- sdf_copy_to(sc, dfjson, overwrite = TRUE)

  expect_equal(
    sdf_nrow(dfjson_tbl),
    3
  )
})

test_that("sdf_copy_to can preserve list columns", {
  if (!"sparklyr.nested" %in% installed.packages()) {
    skip("sparklyr.nested not installed.")
  }

  if (spark_version(sc) < "2.4") {
    skip("preserving list columns is only supported with Spark 2.4+")
  }

  df <- tibble::tibble(
    a = list(c(11.0, 111.0), c(22.0, 222.0), c(33.0, 333.0)),
    b = list(list(c = 1, d = "a"), list(c = 2, d = "b"), list(c = 3, d = "c"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  expect_equal(
    c(sapply(sparklyr.nested::sdf_select(sdf, b.c) %>% sdf_collect(), c)),
    c(1, 2, 3)
  )
  expect_equal(
    c(sapply(sparklyr.nested::sdf_select(sdf, b.d) %>% sdf_collect(), c)),
    c("a", "b", "c")
  )
  res <- sdf_collect(sdf)
  expect_equivalent(df$a, res$a)
})
