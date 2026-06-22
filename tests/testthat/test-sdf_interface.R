skip_connection("sdf_interface")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

# Java numeric bounds used by the spark_write_rds() round-trip tests. Wrapped in
# a helper so that no Spark calls run at file-load time (i.e. outside test_that).
java_numeric_bounds <- function(sc) {
  list(
    double_min = invoke_static(sc, "java.lang.Double", "MIN_VALUE"),
    double_max = invoke_static(sc, "java.lang.Double", "MAX_VALUE"),
    float_min = invoke_static(sc, "java.lang.Float", "MIN_VALUE"),
    float_max = invoke_static(sc, "java.lang.Float", "MAX_VALUE")
  )
}

test_that("spark_write_rds() works as expected with non-array columns", {
  test_requires_version("3.0.0")
  skip_on_arrow()
  skip_databricks_connect()

  bounds <- java_numeric_bounds(sc)
  jdouble.min <- bounds$double_min
  jdouble.max <- bounds$double_max
  jfloat.min <- bounds$float_min
  jfloat.max <- bounds$float_max
  test_rds_output <- tempfile(fileext = ".rds")

  test_lgl_vals <- c(TRUE, NA, FALSE, NA, TRUE, NA, FALSE, NA)
  test_int_vals <- c(
    NA_integer_,
    -2147483647L,
    -2L,
    -1L,
    0L,
    1L,
    2L,
    2147483647L
  )
  test_double_vals <- c(
    NA_real_,
    jdouble.min,
    jdouble.max,
    0,
    1.234,
    -jdouble.min,
    -jdouble.max,
    NaN
  )
  test_float_vals <- c(
    NA_real_,
    jfloat.min,
    jfloat.max,
    0,
    1.234,
    -jfloat.min,
    -jfloat.max,
    NaN
  )
  test_string_vals <- c(
    "abcDEF",
    "a",
    "A",
    "",
    NA,
    "Hello, world!",
    "\001\002\003",
    NA
  )
  test_date_vals <- c(
    as.Date(2500 * seq(4), origin = "1970-01-01", tz = "UTC"),
    as.Date(NA_integer_, origin = "1970-01-01", tz = "UTC"),
    as.Date(10000 + 1000 * seq(3), origin = "1970-01-01", tz = "UTC")
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
    dplyr::tibble(
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
      byte_vals = dplyr::sql(
        "TRY_CAST(MOD(`int_vals`, 256) * 77 + 20 AS BYTE)"
      ),
      short_vals = dplyr::sql(
        "TRY_CAST(MOD(`int_vals`, 256) * 77 + 20 AS SHORT)"
      ),
      long_vals = dplyr::sql("TRY_CAST(`int_vals` AS LONG) * 1073741824L")
    )

  spark_write_rds(sdf, paste0("file://", test_rds_output))

  actual <- collect_from_rds(test_rds_output)

  expect_equal(actual %>% dplyr::pull(boolean_vals), test_lgl_vals)
  expect_equal(actual %>% dplyr::pull(int_vals), test_int_vals)
  expect_equal(actual %>% dplyr::pull(double_vals), test_double_vals)
  expect_equal(
    actual %>% dplyr::pull(float_vals),
    sdf %>% dplyr::pull(float_vals)
  )
  expect_equal(actual %>% dplyr::pull(string_vals), test_string_vals)
  expect_equal(actual %>% dplyr::pull(date_vals), test_date_vals)
  expect_equal(
    actual %>% dplyr::pull(decimal_vals),
    sdf %>% dplyr::pull(decimal_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(byte_vals),
    sdf %>% dplyr::pull(byte_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(short_vals),
    sdf %>% dplyr::pull(short_vals)
  )
  expect_equal(
    actual %>% dplyr::pull(long_vals),
    sdf %>% dplyr::pull(long_vals)
  )
  expect_equal(actual %>% dplyr::pull(struct_vals), test_struct_vals)
})

test_that("spark_write_rds() works as expected with array columns", {
  test_requires_version("3.0.0")
  skip_on_arrow()
  skip_databricks_connect()

  bounds <- java_numeric_bounds(sc)
  jfloat.min <- bounds$float_min
  jfloat.max <- bounds$float_max
  test_rds_output <- tempfile(fileext = ".rds")

  test_lgl_arr <- list(
    TRUE,
    FALSE,
    NA,
    c(FALSE, NA, TRUE, NA, FALSE, NA, NA, TRUE, FALSE),
    c(FALSE, TRUE, TRUE, NA, FALSE, FALSE, FALSE, NA, TRUE, FALSE, FALSE, TRUE),
    rep(NA, 7),
    c(TRUE, NA, NA, NA, FALSE)
  )

  test_long_arr <- list(
    1234L,
    0L,
    -1234L,
    NA_integer_,
    c(142L, 85L, NA_integer_, 714L, NA_integer_, 2857L, NA_integer_),
    c(
      NA_integer_,
      -2147483647L,
      -2L,
      -1L,
      0L,
      1L,
      2L,
      2147483647L,
      NA_integer_
    ),
    rep(NA_integer_, 7)
  )

  test_double_arr <- list(
    jfloat.min,
    jfloat.max,
    NA_real_,
    rep(NA_real_, 7),
    0.0,
    c(-1.234e10, 1.234e10, NA_real_, jfloat.min, jfloat.max),
    c(1.432e5, 8.765, NA_real_, -714.2857, NA_real_, -1.432e10, NA_real_)
  )

  test_string_arr <- list(
    c("a", "b", "c", NA_character_, ""),
    "",
    rep("", 7),
    rep(NA_character_, 7),
    c("Hello", ",", NA_character_, " ", "", "world", NA_character_, "!", ""),
    c("", NA_character_, "", "", NA_character_),
    "Hello"
  )

  test_long_arr <- list(
    1234L,
    0L,
    -1234L,
    NA_integer_,
    c(142L, 85L, NA_integer_, 714L, NA_integer_, 2857L, NA_integer_),
    c(
      NA_integer_,
      -2147483647L,
      -2L,
      -1L,
      0L,
      1L,
      2L,
      2147483647L,
      NA_integer_
    ),
    rep(NA_integer_, 7)
  )

  test_date_arr <- list(
    as.Date(2500 * seq(4), origin = "1970-01-01", tz = "UTC"),
    as.Date(NA_integer_, origin = "1970-01-01", tz = "UTC"),
    as.Date(rep(NA_integer_, 7), origin = "1970-01-01", tz = "UTC"),
    as.Date(10000 + 1000 * seq(3), origin = "1970-01-01", tz = "UTC"),
    as.Date(c(1, NA_integer_, 2), origin = "1970-01-01", tz = "UTC"),
    as.Date(NULL, origin = "1970-01-01", tz = "UTC"),
    as.Date(1, origin = "1970-01-01", tz = "UTC")
  )

  arr_sdf <- copy_to(
    sc,
    dplyr::tibble(
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
        "TRANSFORM(`long_arr`, x -> TRY_CAST(`x` AS BYTE))"
      )
    ) %>%
    dplyr::mutate(
      short_arr = dplyr::sql(
        "TRANSFORM(`long_arr`, x -> TRY_CAST(`x` AS SHORT))"
      )
    ) %>%
    dplyr::mutate(
      float_arr = dplyr::sql(
        "TRANSFORM(`double_arr`, x -> TRY_CAST(`x` AS FLOAT))"
      )
    ) %>%
    dplyr::mutate(
      decimal_arr = dplyr::sql(
        "TRANSFORM(`long_arr`, x -> TRY_CAST(`x` AS DECIMAL))"
      )
    )

  spark_write_rds(arr_sdf, paste0("file://", test_rds_output))

  actual <- collect_from_rds(test_rds_output)

  expect_equal(actual %>% dplyr::pull(lgl_arr), test_lgl_arr)
  expect_equal(actual %>% dplyr::pull(long_arr), test_long_arr)
  expect_equal(actual %>% dplyr::pull(double_arr), test_double_arr)
  expect_equal(actual %>% dplyr::pull(string_arr), test_string_arr)
  expect_equal(actual %>% dplyr::pull(date_arr), test_date_arr)
  expect_equal(
    actual %>% dplyr::pull(byte_arr),
    arr_sdf %>% dplyr::pull(byte_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(short_arr),
    arr_sdf %>% dplyr::pull(short_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(float_arr),
    arr_sdf %>% dplyr::pull(float_arr)
  )
  expect_equal(
    actual %>% dplyr::pull(decimal_arr),
    arr_sdf %>% dplyr::pull(decimal_arr)
  )
})

test_that("spark_write_rds() works as expected with multiple Spark dataframe partitions", {
  test_requires_version("3.0.0")
  skip_on_arrow()
  skip_databricks_connect()

  test_requires("nycflights13")

  num_partitions <- 10L
  flights_sdf <- copy_to(sc, flights, repartition = num_partitions)

  dest_uri <- paste0(
    "file://",
    tempfile(pattern = "flights-part-{partitionId}-")
  )
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

  actual_df <- do.call(rbind, partitions)
  flights_df <- flights
  # ignore timezone attributes in expect_equivalent() comparison
  attributes(flights_df$time_hour) <- attributes(actual_df$time_hour)
  expect_equivalent(actual_df, flights_df)
})

# ---- Coverage tests for R/sdf_interface.R ------------------------------------

test_that("sdf_sort() sorts by one and by multiple columns", {
  # `a` has a tie (two 2L rows) so the secondary `b` key is actually exercised
  df <- sdf_copy_to(
    sc,
    data.frame(a = c(2L, 1L, 2L), b = c("z", "y", "x"), stringsAsFactors = FALSE),
    name = random_string("test_sdf_sort_"),
    overwrite = TRUE
  )

  one <- sdf_sort(df, "a") %>% dplyr::collect()
  expect_equal(one$a, c(1L, 2L, 2L))

  multi <- sdf_sort(df, c("a", "b")) %>% dplyr::collect()
  expect_equal(multi$a, c(1L, 2L, 2L))
  # within the a == 2 tie, b breaks the tie ascending: "x" before "z"
  expect_equal(multi$b, c("y", "x", "z"))
})

test_that("sdf_sort() errors when no columns are supplied", {
  df <- sdf_copy_to(
    sc, data.frame(a = 1:3),
    name = random_string("test_sdf_sort_err_"), overwrite = TRUE
  )
  expect_error(sdf_sort(df, character(0)), "one or more column names")
})

test_that("sdf_sample() with a seed is reproducible", {
  df <- sdf_copy_to(
    sc, data.frame(id = 1:100),
    name = random_string("test_sdf_sample_"), overwrite = TRUE
  )
  s1 <- sdf_sample(df, fraction = 0.5, replacement = FALSE, seed = 42L) %>%
    dplyr::collect()
  s2 <- sdf_sample(df, fraction = 0.5, replacement = FALSE, seed = 42L) %>%
    dplyr::collect()
  expect_equal(sort(s1$id), sort(s2$id))
})

test_that("sdf_weighted_sample() draws the requested number of rows", {
  df <- sdf_copy_to(
    sc, data.frame(id = 1:100, w = runif(100)),
    name = random_string("test_sdf_wsample_"), overwrite = TRUE
  )
  res <- sdf_weighted_sample(df, weight_col = "w", k = 10, replacement = FALSE) %>%
    dplyr::collect()
  expect_equal(nrow(res), 10)
})

test_that("sdf_persist() forces computation and returns a tbl_spark", {
  df <- sdf_copy_to(
    sc, data.frame(id = 1:10),
    name = random_string("test_sdf_persist_"), overwrite = TRUE
  )
  persisted <- sdf_persist(df, storage.level = "MEMORY_ONLY")
  expect_s3_class(persisted, "tbl_spark")
  expect_equal(sdf_nrow(persisted), 10)
})

test_that("sdf_broadcast() returns an equivalent tbl_spark", {
  df <- sdf_copy_to(
    sc, data.frame(id = 1:10),
    name = random_string("test_sdf_broadcast_"), overwrite = TRUE
  )
  b <- sdf_broadcast(df)
  expect_s3_class(b, "tbl_spark")
  expect_equal(sdf_nrow(b), 10)
})

test_that("sdf_drop_duplicates() removes duplicate rows (all cols and subset)", {
  df <- sdf_copy_to(
    sc,
    data.frame(a = c(1L, 1L, 2L), b = c("x", "x", "y"), stringsAsFactors = FALSE),
    name = random_string("test_sdf_dedup_"), overwrite = TRUE
  )
  expect_equal(nrow(sdf_drop_duplicates(df) %>% dplyr::collect()), 2)
  expect_equal(nrow(sdf_drop_duplicates(df, cols = "a") %>% dplyr::collect()), 2)
})

test_that("sdf_drop_duplicates() errors on unknown columns", {
  df <- sdf_copy_to(
    sc, data.frame(a = 1:3),
    name = random_string("test_sdf_dedup_err_"), overwrite = TRUE
  )
  expect_error(sdf_drop_duplicates(df, cols = "nope"), "not in the data frame")
})

test_that("sdf_register() registers a Spark jobj under a given name", {
  df <- sdf_copy_to(
    sc, data.frame(id = 1:5),
    name = random_string("test_sdf_register_"), overwrite = TRUE
  )
  nm <- random_string("sdf_reg_")
  registered <- sdf_register(spark_dataframe(df), name = nm)
  expect_s3_class(registered, "tbl_spark")
  expect_true(nm %in% src_tbls(sc))
})

test_clear_cache()
