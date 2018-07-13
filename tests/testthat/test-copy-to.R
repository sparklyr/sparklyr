context("copy data")

sc <- testthat_spark_connection()

test_that("sdf_copy_to works for default serializer", {
  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_data_frame()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("sdf_copy_to works for scala serializer", {
  skip_livy()

  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_data_frame()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE, serializer = "csv_file_scala")

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("sdf_copy_to works for csv serializer", {
  skip_livy()

  df <- matrix(0, ncol = 5, nrow = 2) %>% dplyr::as_data_frame()
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
