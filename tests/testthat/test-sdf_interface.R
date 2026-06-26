skip_connection("sdf_interface")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()
test_requires("dplyr")

var1 <- seq(26)
var2 <- letters
var3 <- c(1.111, 3.3, 2.22222, NaN, 4.4444, 5.5)
var4 <- c("foo", NA, "bar", "", "baz", NA)

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
    data.frame(
      a = c(2L, 1L, 2L),
      b = c("z", "y", "x"),
      stringsAsFactors = FALSE
    ),
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
    sc,
    data.frame(a = 1:3),
    name = random_string("test_sdf_sort_err_"),
    overwrite = TRUE
  )
  expect_error(sdf_sort(df, character(0)), "one or more column names")
})

test_that("sdf_sample() with a seed is reproducible", {
  df <- sdf_copy_to(
    sc,
    data.frame(id = 1:100),
    name = random_string("test_sdf_sample_"),
    overwrite = TRUE
  )
  s1 <- sdf_sample(df, fraction = 0.5, replacement = FALSE, seed = 42L) %>%
    dplyr::collect()
  s2 <- sdf_sample(df, fraction = 0.5, replacement = FALSE, seed = 42L) %>%
    dplyr::collect()
  expect_equal(sort(s1$id), sort(s2$id))
})

test_that("sdf_weighted_sample() draws the requested number of rows", {
  df <- sdf_copy_to(
    sc,
    data.frame(id = 1:100, w = runif(100)),
    name = random_string("test_sdf_wsample_"),
    overwrite = TRUE
  )
  res <- sdf_weighted_sample(
    df,
    weight_col = "w",
    k = 10,
    replacement = FALSE
  ) %>%
    dplyr::collect()
  expect_equal(nrow(res), 10)
})

test_that("sdf_persist() forces computation and returns a tbl_spark", {
  df <- sdf_copy_to(
    sc,
    data.frame(id = 1:10),
    name = random_string("test_sdf_persist_"),
    overwrite = TRUE
  )
  persisted <- sdf_persist(df, storage.level = "MEMORY_ONLY")
  expect_s3_class(persisted, "tbl_spark")
  expect_equal(sdf_nrow(persisted), 10)
})

test_that("sdf_broadcast() returns an equivalent tbl_spark", {
  df <- sdf_copy_to(
    sc,
    data.frame(id = 1:10),
    name = random_string("test_sdf_broadcast_"),
    overwrite = TRUE
  )
  b <- sdf_broadcast(df)
  expect_s3_class(b, "tbl_spark")
  expect_equal(sdf_nrow(b), 10)
})

test_that("sdf_drop_duplicates() removes duplicate rows (all cols and subset)", {
  df <- sdf_copy_to(
    sc,
    data.frame(
      a = c(1L, 1L, 2L),
      b = c("x", "x", "y"),
      stringsAsFactors = FALSE
    ),
    name = random_string("test_sdf_dedup_"),
    overwrite = TRUE
  )
  expect_equal(nrow(sdf_drop_duplicates(df) %>% dplyr::collect()), 2)
  expect_equal(
    nrow(sdf_drop_duplicates(df, cols = "a") %>% dplyr::collect()),
    2
  )
})

test_that("sdf_drop_duplicates() errors on unknown columns", {
  df <- sdf_copy_to(
    sc,
    data.frame(a = 1:3),
    name = random_string("test_sdf_dedup_err_"),
    overwrite = TRUE
  )
  expect_error(sdf_drop_duplicates(df, cols = "nope"), "not in the data frame")
})

test_that("sdf_register() registers a Spark jobj under a given name", {
  df <- sdf_copy_to(
    sc,
    data.frame(id = 1:5),
    name = random_string("test_sdf_register_"),
    overwrite = TRUE
  )
  nm <- random_string("sdf_reg_")
  registered <- sdf_register(spark_dataframe(df), name = nm)
  expect_s3_class(registered, "tbl_spark")
  expect_true(nm %in% src_tbls(sc))
})

test_that("sdf_describe() works properly", {
  iris_tbl <- testthat_tbl("iris")
  s <- sdf_describe(iris_tbl)
  expect_equal(colnames(s), c("summary", colnames(iris_tbl)))
  expect_equal(pull(s, summary), c("count", "mean", "stddev", "min", "max"))

  s <- sdf_describe(iris_tbl, "Sepal_Length")
  expect_equal(colnames(s), c("summary", "Sepal_Length"))
})

test_that("sdf_describe() checks column name", {
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    sdf_describe(iris_tbl, c("Sepal_Length", "foo")),
    "The following columns are not in the data frame: foo"
  )
})

test_that("sdf_describe() works properly", {
  iris_tbl <- testthat_tbl("iris")

  s <- iris_tbl %>%
    select(Petal_Length, Species) %>%
    sdf_drop_duplicates()

  expect_equal(sdf_nrow(s), 48)

  s <- iris_tbl %>% sdf_drop_duplicates(c("Species"))
  expect_equal(sdf_nrow(s), 3)
})

test_that("sdf_drop_duplicates() checks column name", {
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    sdf_drop_duplicates(iris_tbl, c("Sepal_Length", "foo")),
    "The following columns are not in the data frame: foo"
  )
})

test_that("sdf_partition_sizes works as expected", {
  num_rows <- 100L
  num_partitions <- 10L

  sdf <- sdf_len(sc, num_rows, repartition = num_partitions)
  rs <- sdf_partition_sizes(sdf)

  expect_equal(length(rs$partition_size), num_partitions)
  expect_equal(sum(rs$partition_size), num_rows)
})

test_that("to_avro and from_avro work properly", {
  test_requires_version("2.4.0", max_version = "4")
  skip_databricks_connect()

  df <- dplyr::tibble(
    student = list(
      list(name = "Alice", id = 1L, grade = 3.9),
      list(name = "Bob", id = 2L, grade = 3.7),
      list(name = "Carol", id = 3L, grade = 4.0)
    )
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  sdf_transformed <- sdf_to_avro(sdf)
  sdf_transformed %>%
    sdf_collect() %>%
    (function(collected) {
      expect_equal(colnames(collected), c("student"))
      expect_equal(typeof(collected$student), "list")
      expect_equal(typeof(collected$student[[1]]), "raw")
    })

  schema <- list(
    type = "record",
    name = "topLevelRecord",
    fields = list(
      list(
        name = "student",
        type = list(
          list(
            type = "record",
            name = "student",
            namespace = "topLevelRecord",
            fields = list(
              list(name = "grade", type = list("double", "null")),
              list(name = "id", type = list("long", "null")),
              list(name = "name", type = list("string", "null"))
            )
          ),
          "null"
        )
      )
    )
  )

  expect_warning_on_arrow(
    collected <- sdf_from_avro(
      sdf_transformed,
      c(
        student = schema %>%
          jsonlite::toJSON(auto_unbox = TRUE) %>%
          as.character()
      )
    ) %>%
      sdf_collect()
  )

  expect_equal(colnames(collected), "student")
  expect_equal(length(collected$student), length(df$student))
  for (i in seq_along(collected$student)) {
    actual <- collected$student[[i]]$student
    expected <- df$student[[i]]
    expect_equal(
      names(actual),
      c("grade", "id", "name")
    )
    for (attr in c("grade", "id", "name")) {
      expect_equal(actual[[attr]], expected[[attr]])
    }
  }
})

test_that("sdf_repartition works", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(
    iris_tbl %>%
      sdf_repartition(4L) %>%
      sdf_num_partitions(),
    4L
  )
})

test_that("sdf_reparition: partitioning by column works", {
  test_requires_version("2.0.0", "partitioning by column requires spark 2.0+")
  iris_tbl <- testthat_tbl("iris")

  if (is_testing_databricks_connect()) {
    # DB Connect's optimized plans don't display much useful information when calling toString,
    # so we use the analyzed plan instead
    plan_type <- "analyzed"
  } else {
    plan_type <- "optimizedPlan"
  }

  expect_true(
    any(grepl(
      "RepartitionByExpression \\[Species, Petal_Width\\]",
      iris_tbl %>%
        sdf_repartition(partition_by = c("Species", "Petal_Width")) %>%
        sdf_query_plan(plan_type) %>%
        gsub("#[0-9]+", "", ., perl = TRUE)
    ))
  )

  expect_true(
    any(grepl(
      "RepartitionByExpression \\[Species, Petal_Width\\], 5",
      iris_tbl %>%
        sdf_repartition(5L, partition_by = c("Species", "Petal_Width")) %>%
        sdf_query_plan(plan_type) %>%
        gsub("#[0-9]+", "", ., perl = TRUE)
    ))
  )
})

test_that("'sdf_partition' -- 'partitions' argument should take numeric (#735)", {
  test_requires_version("2.0.0", "partitioning by column requires spark 2.0+")
  iris_tbl <- testthat_tbl("iris")
  expect_equal(
    iris_tbl %>%
      sdf_repartition(6, partition_by = "Species") %>%
      sdf_num_partitions(),
    6
  )
})

test_that("'sdf_coalesce' works as expected", {
  iris_tbl <- testthat_tbl("iris")

  expect_equal(
    iris_tbl %>%
      sdf_repartition(5) %>%
      sdf_coalesce(1) %>%
      sdf_num_partitions(),
    1
  )
})

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
    expand.grid(
      x = var1,
      y = var2,
      z = var3,
      w = var4,
      stringsAsFactors = FALSE
    )
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

local({
  sample_space_sz <- 100L
  num_zeroes <- 50L

  weighted_sampling_test_data <- data.frame(
    id = seq(sample_space_sz + num_zeroes),
    weight = c(
      rep(1, 50),
      rep(2, 25),
      rep(4, 10),
      rep(8, 10),
      rep(16, 5),
      rep(0, num_zeroes)
    )
  )
  sdf <- testthat_tbl(
    name = "weighted_sampling_test_data",
    repartition = 5L
  )

  sample_sz <- 20L
  num_sampling_iters <- 50L
  alpha <- 0.05

  verify_distribution <- function(replacement) {
    expected_dist <- rep(0L, sample_space_sz)
    actual_dist <- rep(0L, sample_space_sz)

    for (x in seq(num_sampling_iters)) {
      seed <- 142857L + x
      set.seed(seed)

      sample <- weighted_sampling_test_data %>%
        dplyr::slice_sample(
          n = sample_sz,
          weight_by = weight,
          replace = replacement
        )
      for (id in sample$id) {
        expected_dist[[id]] <- expected_dist[[id]] + 1L
      }

      sample <- sdf %>%
        sdf_weighted_sample(
          k = sample_sz,
          weight_col = "weight",
          replacement = replacement,
          seed = seed + x
        ) %>%
        collect()
      for (id in sample$id) {
        actual_dist[[id]] <- actual_dist[[id]] + 1L
      }
    }

    expect_warning(
      res <- ks.test(x = actual_dist, y = expected_dist)
    )

    expect_gte(res$p.value, alpha)
  }

  test_that("sdf_weighted_sample without replacement works as expected", {
    verify_distribution(replacement = FALSE)
  })

  test_that("sdf_weighted_sample with replacement works as expected", {
    verify_distribution(replacement = TRUE)
  })

  test_that("sdf_weighted_sample returns repeatable results from a fixed PRNG seed", {
    seed <- 142857L
    for (replacement in c(TRUE, FALSE)) {
      samples <- lapply(
        seq(2),
        function(x) {
          sdf %>%
            sdf_weighted_sample(
              weight_col = "weight",
              k = sample_sz,
              replacement = replacement,
              seed = seed
            ) %>%
            collect()
        }
      )

      expect_equivalent(
        samples[[1]] %>% dplyr::arrange(id),
        samples[[2]] %>% dplyr::arrange(id)
      )
    }
  })
})

local({
  weighted_sampling_octal_test_data <- data.frame(
    x = rep(seq(0L, 7L), 100L),
    weight = 1L + (seq(800L) * 7L + 11L) %% 17L
  )

  sdf <- testthat_tbl(
    name = "weighted_sampling_octal_test_data",
    repartition = 4L
  )

  num_sampling_iters <- 100L
  alpha <- 0.05

  sample_sz <- 3L

  # map each possible outcome to an octal value
  to_oct <- function(sample) {
    sum(8L^seq(0L, sample_sz - 1L) * sample$x)
  }

  max_possible_outcome <- to_oct(list(x = rep(7, sample_sz)))

  verify_distribution <- function(replacement) {
    expected_dist <- rep(0L, max_possible_outcome + 1L)
    actual_dist <- rep(0L, max_possible_outcome + 1L)

    for (x in seq(num_sampling_iters)) {
      seed <- x * 97L
      set.seed(seed)

      sample <- weighted_sampling_octal_test_data %>%
        dplyr::slice_sample(
          n = sample_sz,
          weight_by = weight,
          replace = replacement
        ) %>%
        to_oct()
      expected_dist[[sample + 1L]] <- expected_dist[[sample + 1L]] + 1L

      sample <- sdf %>%
        sdf_weighted_sample(
          k = sample_sz,
          weight_col = "weight",
          replacement = replacement,
          seed = seed
        ) %>%
        collect() %>%
        to_oct()
      actual_dist[[sample + 1L]] <- actual_dist[[sample + 1L]] + 1L
    }

    expect_warning(
      res <- ks.test(x = actual_dist, y = expected_dist)
    )

    expect_gte(res$p.value, alpha)
  }

  test_that("sdf_weighted_sample without replacement works as expected", {
    verify_distribution(replacement = FALSE)
  })

  test_that("sdf_weighted_sample with replacement works as expected", {
    verify_distribution(replacement = TRUE)
  })
})

test_clear_cache()
