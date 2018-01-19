context("copy data")
sc <- testthat_spark_connection()

test_that("sdf_copy_to works for scala serializer", {
  skip_on_cran()

  df <- matrix(0, ncol = 5, nrow = 2) %>% as_data_frame()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE, serializer = "csv_file_scala")

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})

test_that("sdf_copy_to works for csv serializer", {
  skip_on_cran()

  df <- matrix(0, ncol = 5, nrow = 2) %>% as_data_frame()
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE, serializer = "csv_file")

  expect_equal(
    sdf_nrow(df_tbl),
    2
  )
})
