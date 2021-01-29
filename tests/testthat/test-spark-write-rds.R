context("spark write rds")

sc <- testthat_spark_connection()

jdouble.min <- invoke_static(sc, "java.lang.Double", "MIN_VALUE")
jdouble.max <- invoke_static(sc, "java.lang.Double", "MAX_VALUE")
jfloat.min <- invoke_static(sc, "java.lang.Float", "MIN_VALUE")
jfloat.max <- invoke_static(sc, "java.lang.Float", "MAX_VALUE")

test_rds_output <- "/tmp/test.rds"

test_that("spark_write_rds() works as expected with non-array columns", {
  test_requires_version("2.4.0")
  skip_on_arrow()
  skip_databricks_connect()

  test_lgl_vals <- c(TRUE, NA, FALSE, NA, TRUE, NA, FALSE, NA)
  test_int_vals <- c(NA_integer_, -2147483647L, -2L, -1L, 0L, 1L, 2L, 2147483647L)
  test_double_vals <- c(NA_real_, jdouble.min, jdouble.max, 0, 1.234, -jdouble.min, -jdouble.max, NaN)
  test_float_vals <- c(NA_real_, jfloat.min, jfloat.max, 0, 1.234, -jfloat.min, -jfloat.max, NaN)
  test_string_vals <- c("abcDEF", "a", "A", "", NA, "Hello, world!", "\001\002\003", NA)
  test_date_vals = c(
    as.Date(2500 * seq(4), origin = "1970-01-01"),
    as.Date(NA_integer_, origin = "1970-01-01"),
    as.Date(10000 + 1000 * seq(3), origin = "1970-01-01")
  )
  test_struct_vals <- list(
    list(
      a = 1.1,
      b = c(TRUE, FALSE, NA),
      c = c(NA_character_, "foo"),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 2.2,
      b = c(NA, NA, TRUE),
      c = c(rep("", 7), NA_character_),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 3.3,
      b = c(FALSE, TRUE, TRUE),
      c = rep("foo", 2),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 4.4,
      b = c(TRUE, FALSE, NA),
      c = c(NA_character_, "foo"),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 5.5,
      b = c(NA, NA, NA, FALSE),
      c = rep("bar", 10),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 6.6,
      b = c(FALSE, TRUE, TRUE),
      c = c("foo", "bar"),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 7.7,
      b = rep(NA, 7),
      c = c("foo", "bar", "baz"),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    ),
    list(
      a = 8.8,
      b = rep(TRUE, 7),
      c = rep(" ", 8),
      d = list(a = 1L, b = c(2L, 3L), c = "abcd")
    )
  )

  sdf <- copy_to(
    sc,
    tibble::tibble(
      boolean_vals = test_lgl_vals,
      int_vals = test_int_vals,
      double_vals = test_double_vals,
      float_vals = test_float_vals,
      string_vals = test_string_vals,
      date_vals = test_date_vals,
      struct_vals = test_struct_vals
    )
  ) %>%
    dplyr::mutate(
      float_vals = float(float_vals),
      decimal_vals = decimal(int_vals),
      byte_vals = dplyr::sql("CAST(MOD(`int_vals`, 256) * 77 + 20 AS BYTE)"),
      short_vals = dplyr::sql("CAST(MOD(`int_vals`, 256) * 77 + 20 AS SHORT)"),
      long_vals = dplyr::sql("CAST(`int_vals` AS LONG) * 1073741824L")
    )

  spark_write_rds(sdf, paste0("file://", test_rds_output))

  actual <- collect_from_rds(test_rds_output)

  expect_equal(actual %>% dplyr::pull(boolean_vals), test_lgl_vals)
  expect_equal(actual %>% dplyr::pull(int_vals), test_int_vals)
  expect_equal(actual %>% dplyr::pull(double_vals), test_double_vals)
  expect_equal(
    actual %>% dplyr::pull(float_vals), sdf %>% dplyr::pull(float_vals)
  )
  expect_equal(actual %>% dplyr::pull(string_vals), test_string_vals)
  expect_equal(actual %>% dplyr::pull(date_vals), test_date_vals)
  expect_equal(
    actual %>% dplyr::pull(decimal_vals), sdf %>% dplyr::pull(decimal_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(byte_vals), sdf %>% dplyr::pull(byte_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(short_vals), sdf %>% dplyr::pull(short_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(long_vals), sdf %>% dplyr::pull(long_vals)
  )
  expect_equal(actual %>% dplyr::pull(struct_vals), test_struct_vals)
})

test_that("spark_write_rds() works as expected with array columns", {
  test_requires_version("2.4.0")
  skip_on_arrow()
  skip_databricks_connect()

  test_lgl_arr = list(
    TRUE,
    FALSE,
    NA,
    c(FALSE, NA, TRUE, NA, FALSE, NA, NA, TRUE, FALSE),
    c(FALSE, TRUE, TRUE, NA, FALSE, FALSE, FALSE, NA, TRUE, FALSE, FALSE, TRUE),
    rep(NA, 7),
    c(TRUE, NA, NA, NA, FALSE)
  )

  test_long_arr = list(
    1234L,
    0L,
    -1234L,
    NA_integer_,
    c(142L, 85L, NA_integer_, 714L, NA_integer_, 2857L, NA_integer_),
    c(NA_integer_, -2147483647L, -2L, -1L, 0L, 1L, 2L, 2147483647L, NA_integer_),
    rep(NA_integer_, 7)
  )

  test_double_arr = list(
    jfloat.min,
    jfloat.max,
    NA_real_,
    rep(NA_real_, 7),
    0.0,
    c(-1.234e10, 1.234e10, NA_real_, jfloat.min, jfloat.max),
    c(1.432e5, 8.765, NA_real_, -714.2857, NA_real_, -1.432e10, NA_real_)
  )

  test_string_arr = list(
    c("a", "b", "c", NA_character_, ""),
    "",
    rep("", 7),
    rep(NA_character_, 7),
    c("Hello", ",", NA_character_, " ", "", "world", NA_character_, "!", ""),
    c("", NA_character_, "", "", NA_character_),
    "Hello"
  )

  test_long_arr = list(
    1234L,
    0L,
    -1234L,
    NA_integer_,
    c(142L, 85L, NA_integer_, 714L, NA_integer_, 2857L, NA_integer_),
    c(NA_integer_, -2147483647L, -2L, -1L, 0L, 1L, 2L, 2147483647L, NA_integer_),
    rep(NA_integer_, 7)
  )

  test_date_arr = list(
    as.Date(2500 * seq(4), origin = "1970-01-01"),
    as.Date(NA_integer_, origin = "1970-01-01"),
    as.Date(rep(NA_integer_, 7), origin = "1970-01-01"),
    as.Date(10000 + 1000 * seq(3), origin = "1970-01-01"),
    as.Date(c(1, NA_integer_, 2), origin = "1970-01-01"),
    as.Date(NULL, origin = "1970-01-01"),
    as.Date(1, origin = "1970-01-01")
  )

  arr_sdf <- copy_to(
    sc,
    tibble::tibble(
      lgl_arr = test_lgl_arr,
      long_arr = test_long_arr,
      double_arr = test_double_arr,
      string_arr = test_string_arr,
      date_arr = test_date_arr
    )
  ) %>%
    dplyr::mutate(
      date_arr = dplyr::sql(
        "TRANSFORM(`date_arr`, x -> CAST(`x` AS DATE))"
      )
    ) %>%
    dplyr::mutate(
      byte_arr = dplyr::sql(
        "TRANSFORM(`long_arr`, x -> CAST(`x` AS BYTE))"
      )
    ) %>%
    dplyr::mutate(
      short_arr = dplyr::sql(
        "TRANSFORM(`long_arr`, x -> CAST(`x` AS SHORT))"
      )
    ) %>%
    dplyr::mutate(
      float_arr = dplyr::sql(
        "TRANSFORM(`double_arr`, x -> CAST(`x` AS FLOAT))"
      )
    ) %>%
    dplyr::mutate(
      decimal_arr = transform(long_arr, ~ decimal(.x))
    )

  spark_write_rds(arr_sdf, paste0("file://", test_rds_output))

  actual <- collect_from_rds(test_rds_output)

  expect_equal(actual %>% dplyr::pull(lgl_arr), test_lgl_arr)
  expect_equal(actual %>% dplyr::pull(long_arr), test_long_arr)
  expect_equal(actual %>% dplyr::pull(double_arr), test_double_arr)
  expect_equal(actual %>% dplyr::pull(string_arr), test_string_arr)
  expect_equal(actual %>% dplyr::pull(date_arr), test_date_arr)
  expect_equal(
    actual %>% dplyr::pull(byte_arr), arr_sdf %>% dplyr::pull(byte_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(short_arr), arr_sdf %>% dplyr::pull(short_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(float_arr), arr_sdf %>% dplyr::pull(float_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(decimal_arr), arr_sdf %>% dplyr::pull(decimal_arr)
  )
})

test_that("spark_write_rds() works as expected with multiple Spark dataframe partitions", {
  test_requires_version("2.0.0")
  skip_on_arrow()
  skip_databricks_connect()

  test_requires("nycflights13")

  num_partitions <- 10L
  flights_sdf <- copy_to(sc, flights, repartition = num_partitions)

  dest_uri <- paste0("file://", tempfile(pattern = "flights-part-{partitionId}-"))
  outputs <- spark_write_rds(flights_sdf, dest_uri = dest_uri)

  partitions <- outputs %>%
    dplyr::arrange(partition_id) %>%
    dplyr::pull(uri) %>%
    lapply(
      function(x) {
        expect_equal(substr(x, start = 1, stop = 7), "file://")

        collect_from_rds(substr(x, start = 8, stop = nchar(x)))
      }
    )

  expect_equivalent(do.call(rbind, partitions), flights)
})
