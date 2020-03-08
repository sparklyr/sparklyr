context("copy data")

sc <- testthat_spark_connection()

test_that("sdf_copy_to works for default serializer", {
  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("sdf_copy_to works for scala serializer", {
  skip_livy()

  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE, serializer = "csv_file_scala")

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("sdf_copy_to works for csv serializer", {
  skip_databricks_connect()
  skip_livy()

  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE, serializer = "csv_file")

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("spark_table_name() doesn't warn for multiline expression (#1386)", {
  expect_warning(
    spark_table_name(data.frame(foo = c(1, 2, 3),
               bar = c(2, 1, 3),
               foobar = c("a", "b", "c"))
               ),
    NA
  )
})

test_that("sdf_copy_to supports list of callbacks", {
  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_tibble()
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
