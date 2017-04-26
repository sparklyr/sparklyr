context("binds")
test_requires("dplyr")
sc <- testthat_spark_connection()

df1a <- data_frame(a = 1:3, b = letters[1:3])
df2a <- data_frame(c = letters[1:3], d = letters[24:26])
df3a <- data_frame(e = 1:2, f = letters[1:2])

df1a_tbl <- sdf_copy_to(sc, df1a, "df1a", TRUE, 0L)
df1a_1part_tbl <- sdf_copy_to(sc, df1a, "df1a_1part", TRUE, 1L)
df1a_10part_tbl <- sdf_copy_to(sc, df1a, "df1a_10part", TRUE, 10L)
df2a_tbl <- testthat_tbl("df2a")
df3a_tbl <- testthat_tbl("df3a")

test_that("'sdf_with_sequential_id' works independent of number of partitions", {
  test_requires("dplyr")

  expect_equal(
    df1a_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() - 1.0)
  )

  expect_equal(
    df1a_1part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() - 1.0)
  )

  expect_equal(
    df1a_10part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() - 1.0)
  )
})

test_that("'sdf_last_index' works independent of number of partitions", {
  test_requires("dplyr")

  expect_equal(df1a_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_1part_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_10part_tbl %>% sdf_last_index("a"), 3)
})

test_that("'cbind' works as expected", {
  expect_equal(cbind(df1a_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_equal(cbind(df1a_10part_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_error(cbind(df1a_tbl, df3a_tbl),
               "Not all inputs have the same number of rows")
  expect_error(cbind(df1a_tbl, NULL),
               "Unable to retrieve a Spark DataFrame from object of class NULL")
})
