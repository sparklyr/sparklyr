context("binds")
test_requires("dplyr")
sc <- testthat_spark_connection()

df1a <- data.frame(a = 1:3, b = letters[1:3], stringsAsFactors = FALSE)
df2a <- data.frame(c = letters[1:3], d = letters[24:26], stringsAsFactors = FALSE)
df3a <- data.frame(e = 1:2, f = letters[1:2], stringsAsFactors = FALSE)
df4a <- data.frame(b = letters[4:6], a = 4:6, stringsAsFactors = FALSE)

# sdf helper functions -------------------------------------------------------

test_that("'sdf_with_sequential_id' works independent of number of partitions", {
  df1a_tbl <- testthat_tbl("df1a")
  df1a_1part_tbl <- testthat_tbl("df1a_1part", data = df1a, repartition = 1L)
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)

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
  df1a_tbl <- testthat_tbl("df1a")

  expect_equal(df1a_tbl %>% sdf_with_sequential_id(from = 0L) %>% collect(),
               df1a %>% mutate(id = row_number() - 1.0))
  expect_equal(df1a_tbl %>% sdf_with_sequential_id(from = -1L) %>% collect(),
               df1a %>% mutate(id = row_number() - 2.0))
})

test_that("'sdf_last_index' works independent of number of partitions", {
  df1a_tbl <- testthat_tbl("df1a")
  df1a_1part_tbl <- testthat_tbl("df1a_1part", data = df1a, repartition = 1L)
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)

  expect_equal(df1a_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_1part_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_10part_tbl %>% sdf_last_index("a"), 3)
})

# cols -------------------------------------------------------

test_that("'cbind' works as expected", {
  df1a_tbl <- testthat_tbl("df1a")
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  expect_equal(cbind(df1a_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_equal(cbind(df1a_10part_tbl, df2a_tbl) %>% collect(),
               cbind(df1a, df2a))
  expect_error(cbind(df1a_tbl, df3a_tbl),
               "All inputs must have the same number of rows.")
  expect_error(cbind(df1a_tbl, NULL),
               "Unable to retrieve a Spark DataFrame from object of class NULL")
})

test_that("'sdf_bind_cols' agrees with 'cbind'", {
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
               cbind(df1a_tbl, df2a_tbl) %>% collect())
  expect_error(sdf_bind_cols(df1a_tbl, df3a_tbl),
               "All inputs must have the same number of rows.")
})

test_that("'sdf_bind_cols' handles lists", {
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")

  expect_equal(sdf_bind_cols(list(df1a_tbl, df2a_tbl)) %>% collect(),
               sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect())
})

test_that("'sdf_bind_cols' ignores NULL", {
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")

  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl, NULL) %>% collect(),
               sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect())
})

test_that("'sdf_bind_cols' supports programming", {
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  fn <- function(...) sdf_bind_cols(...)
  expect_equal(sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
               fn(df1a_tbl, df2a_tbl) %>% collect())
  expect_error(fn(df1a_tbl, df3a_tbl),
               "All inputs must have the same number of rows.")
})

test_that("'sdf_bind_cols' works with overlapping columns'", {
  if (spark_version(sc) < "2.0.0") skip("sdf_bind_cols() workaround not available in 1.6")

  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  spark_df <- sdf_bind_cols(df1a_tbl, df4a_tbl) %>% collect()
  local_df <- cbind(df1a, df2a)

  expect_equal(nrow(spark_df), nrow(local_df))
  expect_equal(ncol(spark_df), ncol(local_df))
})

# rows -------------------------------------------------------

test_that("'rbind.tbl_spark' agrees with local result ", {
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equal(rbind(df1a_tbl, df4a_tbl) %>% collect(),
               rbind(df1a, df4a))
})

test_that("'sdf_bind_rows' agrees with local result", {
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equal(sdf_bind_rows(df1a_tbl, df4a_tbl) %>% collect(),
               bind_rows(df1a, df4a))
  expect_equal(sdf_bind_rows(df1a_tbl, select(df4a_tbl, -a)) %>% collect(),
               bind_rows(df1a, select(df4a, -a)))
  expect_equal(sdf_bind_rows(df1a_tbl, select(df4a_tbl, -b)) %>% collect(),
               bind_rows(df1a, select(df4a, -b)))
})

test_that("'sdf_bind_rows' -- 'id' argument works as expected", {
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equal(sdf_bind_rows(df1a_tbl, df4a_tbl, id = "source") %>% collect(),
               bind_rows(df1a, df4a, .id = "source"))
  expect_equal(sdf_bind_rows(x = df1a_tbl, y = df4a_tbl, id = "source") %>% collect(),
               bind_rows(x = df1a, y = df4a, .id = "source"))
  expect_equal(sdf_bind_rows(x = df1a_tbl, df4a_tbl, id = "source") %>% collect(),
               bind_rows(x = df1a, df4a, .id = "source"))
})

test_that("'sdf_bind_rows' ignores NULL", {
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equal(sdf_bind_rows(list(df1a_tbl, NULL, df4a_tbl)) %>% collect(),
               sdf_bind_rows(list(df1a_tbl, df4a_tbl)) %>% collect())
})

test_that("'sdf_bind_rows' err for non-tbl_spark", {
  df1a_tbl <- testthat_tbl("df1a")

  expect_error(sdf_bind_rows(df1a_tbl, df1a),
               "all inputs must be tbl_spark")
})

test_that("'sdf_bind_rows' handles column type upcasting (#804)", {
  # Need support for NaN ARROW-3615
  skip_on_arrow()
  skip_on_spark_master()

  test_requires("dplyr")

  df5a <- tibble::tibble(year = as.double(2005:2006),
                    count = c(6, NaN),
                    name = c("a", "b"))
  df6a <- tibble::tibble(year = 2007:2008,
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
