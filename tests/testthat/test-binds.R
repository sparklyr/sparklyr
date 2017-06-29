context("binds")
test_requires("dplyr")
sc <- testthat_spark_connection()

df1a <- data_frame(a = 1:3, b = letters[1:3])
df2a <- data_frame(c = letters[1:3], d = letters[24:26])
df3a <- data_frame(e = 1:2, f = letters[1:2])
df4a <- data_frame(b = letters[4:6], a = 4:6)

df1a_tbl <- sdf_copy_to(sc, df1a, "df1a", TRUE, 0L)
df1a_1part_tbl <- sdf_copy_to(sc, df1a, "df1a_1part", TRUE, 1L)
df1a_10part_tbl <- sdf_copy_to(sc, df1a, "df1a_10part", TRUE, 10L)
df2a_tbl <- testthat_tbl("df2a")
df3a_tbl <- testthat_tbl("df3a")
df4a_tbl <- testthat_tbl("df4a")

# sdf helper functions -------------------------------------------------------

test_that("'sdf_with_sequential_id' works independent of number of partitions", {
  expect_equal(
    df1a_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )

  expect_equal(
    df1a_1part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )

  expect_equal(
    df1a_10part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )
})

test_that("'sdf_with_sequential_id' -- 'from' argument works as expected", {
  expect_equal(df1a_tbl %>% sdf_with_sequential_id(from = 0L) %>% collect(),
               df1a %>% mutate(id = row_number() - 1.0))
  expect_equal(df1a_tbl %>% sdf_with_sequential_id(from = -1L) %>% collect(),
               df1a %>% mutate(id = row_number() - 2.0))
})

test_that("'sdf_last_index' works independent of number of partitions", {
  expect_equal(df1a_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_1part_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_10part_tbl %>% sdf_last_index("a"), 3)
})

# cols -------------------------------------------------------

test_that("'cbind' works as expected", {
  expect_equal(cbind(df1a_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_equal(cbind(df1a_10part_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_error(cbind(df1a_tbl, df3a_tbl),
               "Not all inputs have the same number of rows, for example:\\n       tbl nrow\\n1 df3a_tbl    2\\n2 df1a_tbl    3")
  expect_error(cbind(df1a_tbl, NULL),
               "Unable to retrieve a Spark DataFrame from object of class NULL")
})

test_that("'sdf_bind_cols' agrees with 'cbind'", {
  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
               cbind(df1a_tbl, df2a_tbl) %>% collect())
  expect_error(sdf_bind_cols(df1a_tbl, df3a_tbl),
               "Not all inputs have the same number of rows, for example:\\n       tbl nrow\\n1 df3a_tbl    2\\n2 df1a_tbl    3")
})

test_that("'sdf_bind_cols' handles lists", {
  expect_equal(sdf_bind_cols(list(df1a_tbl, df2a_tbl)) %>% collect(),
               sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect())
  expect_equal(sdf_bind_cols(df1a_tbl, list(df2a_tbl)) %>% collect(),
               sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect())
})

test_that("'sdf_bind_cols' ignores NULL", {
  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl, NULL) %>% collect(),
               sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect())
})

test_that("'sdf_bind_cols' supports programming", {
  fn <- function(...) sdf_bind_cols(...)
  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
               fn(df1a_tbl, df2a_tbl) %>% collect())
  expect_error(fn(df1a_tbl, df3a_tbl),
               "Not all inputs have the same number of rows, for example:\\n       tbl nrow\\n1 df3a_tbl    2\\n2 df1a_tbl    3")
})

# rows -------------------------------------------------------

test_that("'rbind.tbl_spark' agrees with local result ", {
  expect_equal(rbind(df1a_tbl, df4a_tbl) %>% collect(),
               rbind(df1a, df4a))
})

test_that("'sdf_bind_rows' agrees with local result", {
  test_requires("dplyr")
  expect_equal(sdf_bind_rows(df1a_tbl, df4a_tbl) %>% collect(),
               bind_rows(df1a, df4a))
  expect_equal(sdf_bind_rows(df1a_tbl, select(df4a_tbl, -a)) %>% collect(),
               bind_rows(df1a, select(df4a, -a)))
  expect_equal(sdf_bind_rows(df1a_tbl, select(df4a_tbl, -b)) %>% collect(),
               bind_rows(df1a, select(df4a, -b)))
})

test_that("'sdf_bind_rows' -- 'id' argument works as expected", {
  expect_equal(sdf_bind_rows(df1a_tbl, df4a_tbl, id = "source") %>% collect(),
               bind_rows(df1a, df4a, .id = "source"))
  expect_equal(sdf_bind_rows(x = df1a_tbl, y = df4a_tbl, id = "source") %>% collect(),
               bind_rows(x = df1a, y = df4a, .id = "source"))
  expect_equal(sdf_bind_rows(x = df1a_tbl, df4a_tbl, id = "source") %>% collect(),
               bind_rows(x = df1a, df4a, .id = "source"))
})

test_that("'sdf_bind_rows' errs for invalid ID", {
  expect_error(sdf_bind_rows(df1a_tbl, df4a_tbl, id = 5),
               "'5' is not a length-one character vector")
})

test_that("'sdf_bind_rows' ignores NULL", {
  expect_equal(sdf_bind_rows(list(df1a_tbl, NULL, df4a_tbl)) %>% collect(),
               sdf_bind_rows(list(df1a_tbl, df4a_tbl)) %>% collect())
})

test_that("'sdf_bind_rows' err for non-tbl_spark", {
  expect_error(sdf_bind_rows(df1a_tbl, df1a),
               "all inputs must be tbl_spark")
})

test_that("'sdf_bind_rows' handles column type upcasting (#804)", {
  test_requires("dplyr")

  df5a <- data_frame(year = as.double(2005:2006),
                    count = c(6, NaN),
                    name = c("a", "b"))
  df6a <- data_frame(year = 2007:2008,
                    name = c("c", "d"),
                    count = c(0, 0))
  df5a_tbl <- testthat_tbl("df5a")
  df6a_tbl <- testthat_tbl("df6a")

  expect_equal(bind_rows(df5a, df6a),
               sdf_bind_rows(df5a_tbl, df6a_tbl) %>%
                 collect())

  expect_equal(bind_rows(df6a, df5a),
               sdf_bind_rows(df6a_tbl, df5a_tbl) %>%
                 collect())
})
