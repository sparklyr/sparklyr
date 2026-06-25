sc <- testthat_spark_connection()
iris_table_name <- random_table_name("iris")

test_avro_schema <- list(
  type = "record",
  name = "topLevelRecord",
  fields = list(
    list(name = "a", type = list("double", "null")),
    list(name = "b", type = list("int", "null")),
    list(name = "c", type = list("string", "null"))
  )
) %>%
  jsonlite::toJSON(auto_unbox = TRUE) %>%
  as.character()

test_that("spark_write_delta() and spark_read_delta() work as expected", {
  skip_on_livy()
  skip_connection("format-delta")
  test_requires_version("3", max_version = "4")
  test_requires("nycflights13")

  flights_df <- flights %>% head(100)
  flights_sdf <- copy_to(sc, flights_df, name = random_string()) %>%
    dplyr::mutate(rownum = dplyr::sql("ROW_NUMBER() OVER (ORDER BY NULL)"))

  path <- tempfile("flights_")
  spark_write_delta(flights_sdf, path)

  expect_equivalent(
    spark_read_delta(sc, path) %>%
      collect() %>%
      dplyr::arrange(rownum),
    flights_sdf %>%
      collect() %>%
      dplyr::arrange(rownum)
  )
})

test_that("spark_read_text() and spark_write_text() read and write basic files", {
  skip_on_livy()
  skip_connection("format-text")
  skip_databricks_connect()
  test_requires("dplyr")

  test_file_path <- tempfile()

  text_file <- file(test_file_path, "w+")
  cat("1\n2\n3", file = text_file)
  close(text_file)

  sdf <- spark_read_text(
    sc,
    name = "test_spark_read",
    path = test_file_path
  )

  output_file <- tempfile()
  spark_write_text(
    sdf,
    path = output_file
  )

  sdf_roundtrip <- spark_read_text(
    sc,
    name = "test_spark_roundtrip",
    path = output_file
  )

  expect_equal(sdf %>% collect(), sdf_roundtrip %>% collect())
})

test_that("spark_write_table() can append data", {
  skip_on_livy()
  skip_connection("format-table")
  skip_databricks_connect()
  if (spark_version(sc) < "2.0.0") {
    skip("tables not supported before 2.0.0")
  }
  test_requires("dplyr")

  iris_tbl <- testthat_tbl("iris")

  spark_write_table(iris_tbl, iris_table_name)
  expect_equal(
    sdf_nrow(tbl(sc, iris_table_name)),
    nrow(iris)
  )

  spark_write_table(iris_tbl, iris_table_name, mode = "append")
  expect_equal(
    sdf_nrow(tbl(sc, iris_table_name)),
    2 * nrow(iris)
  )
})

test_that("spark_write_table() can write data", {
  skip_on_livy()
  skip_connection("format-table")
  skip_databricks_connect()
  if (spark_version(sc) < "2.0.0") {
    skip("tables not supported before 2.0.0")
  }
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(id = 1L))

  tbl <- random_string("test_write_table_new")
  spark_write_table(df, tbl)

  append_table <- tbl(sc, tbl)

  expect_equal(sdf_nrow(append_table), 1)
})

test_that("spark_write_table() overwrites existing table definition when overwriting", {
  skip_on_livy()
  skip_connection("format-table")
  skip_databricks_connect()
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(foo = 1L, bar = 2L))

  tbl <- random_string("test_write_table_new")

  ddl <- glue::glue(
    "CREATE TABLE {tbl} (
    foo INT
    ,bar BIGINT
    )"
  )

  DBI::dbExecute(sc, ddl)

  spark_write_table(df, tbl, mode = "overwrite")

  # The type of bar will be int, rather than bigint, because the table was
  # overwritten from scratch
  desc <- DBI::dbGetQuery(sc, glue::glue("DESCRIBE TABLE {tbl}"))

  expect_equal(
    dplyr::as_tibble(desc),
    dplyr::tribble(
      ~col_name , ~data_type , ~comment      ,
      "foo"     , "int"      , NA_character_ ,
      "bar"     , "int"      , NA_character_ ,
    )
  )

  new_table <- tbl(sc, tbl)

  expect_equal(
    dplyr::as_tibble(new_table),
    dplyr::tribble(
      ~foo , ~bar ,
      1L   , 2L
    )
  )
})

test_that("spark_insert_table() inserts into existing table definition, even when overwriting", {
  skip_on_livy()
  skip_connection("format-insert-table")
  skip_databricks_connect()
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(foo = 1L, bar = 2L))

  tbl <- random_string("test_write_table_new")

  ddl <- glue::glue(
    "CREATE TABLE {tbl} (
    foo INT
    ,bar BIGINT
    )"
  )

  DBI::dbExecute(sc, ddl)

  spark_insert_table(df, tbl, overwrite = TRUE)

  desc <- DBI::dbGetQuery(sc, glue::glue("DESCRIBE TABLE {tbl}"))

  expect_equal(
    dplyr::as_tibble(desc),
    dplyr::tribble(
      ~col_name , ~data_type , ~comment      ,
      "foo"     , "int"      , NA_character_ ,
      "bar"     , "bigint"   , NA_character_ ,
    )
  )

  new_table <- tbl(sc, tbl)

  # bar is returned as a double when the table definition is bigint
  expect_equal(
    dplyr::as_tibble(new_table),
    dplyr::tribble(
      ~foo , ~bar ,
      1L   ,    2
    )
  )
})

test_that("spark_write() works as expected", {
  skip_on_livy()
  skip_connection("format-generalized")
  test_requires_version("2.4.0")

  iris_tbl <- testthat_tbl("iris")

  writer <- function(df, path) {
    list(list(df = df, path = path))
  }

  verify_spark_write_result <- function(res, expected_paths) {
    sort_df <- function(df) df[do.call(order, as.list(df)), ]

    actual <- do.call(rbind, lapply(res, function(e) e$df)) %>% sort_df()

    expected <- iris
    expected$Species <- as.character(expected$Species)
    expected <- sort_df(expected)
    colnames(expected) <- lapply(colnames(expected), function(x) {
      gsub("\\.", "_", x)
    })

    expect_equal(colnames(actual), colnames(expected))
    for (col in colnames(actual)) {
      expect_equal(actual[[col]], expected[[col]])
    }

    expect_equal(lapply(res, function(e) e$path), as.list(expected_paths))
  }

  multiple_paths <- lapply(seq(5), function(x) paste0("hdfs://file_", x))
  single_path <- "hdfs://iris"

  expect_warning_on_arrow(
    for (paths in list(list(multiple_paths), list(single_path))) {
      verify_spark_write_result(
        res = spark_write(
          iris_tbl,
          writer = writer,
          paths = paths[[1]]
        ),
        expected_paths = as.list(paths[[1]])
      )
    }
  )
})

test_that("spark_write_avro() works as expected", {
  skip_on_livy()
  skip_connection("format-avro")
  test_requires_version("2.4.0", max_version = "4")
  skip_databricks_connect()

  df <- dplyr::tibble(
    a = c(1, NaN, 3, 4, NaN),
    b = c(-2L, 0L, 1L, 3L, 6L),
    c = c("ab", "cde", "zzzz", "", "fghi")
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)

  for (avro_schema in list(NULL, test_avro_schema)) {
    path <- tempfile(pattern = "test_spark_write_avro_", fileext = ".avro")
    spark_write_avro(sdf, path = path, avro_schema = avro_schema)
    actual <- spark_read_avro(sc, path = path) %>% sdf_collect()

    expect_equal(colnames(df), colnames(actual))

    for (col in colnames(df)) {
      expect_equal(df[[col]], actual[[col]])
    }
  }
})

test_that("spark read/write methods avoid name collision on identical file names", {
  skip_on_livy()
  skip_connection("format-avro")
  test_requires_version("2.4.0", max_version = "4")

  tbl_1 <- dplyr::tibble(name = c("foo_1", "bar_1"))
  tbl_2 <- dplyr::tibble(name = c("foo_2", "bar_2"))
  sdf_1 <- copy_to(sc, tbl_1)
  sdf_2 <- copy_to(sc, tbl_2)

  impls <- list(
    structure(c(read = spark_read_csv, write = spark_write_csv)),
    structure(c(read = spark_read_parquet, write = spark_write_parquet)),
    structure(c(read = spark_read_json, write = spark_write_json)),
    structure(c(read = spark_read_text, write = spark_write_text)),
    structure(c(read = spark_read_orc, write = spark_write_orc))
  )

  if (!is_testing_databricks_connect()) {
    impls <- append(
      impls,
      list(structure(c(read = spark_read_avro, write = spark_write_avro)))
    )
  }

  for (impl in impls) {
    path1 <- tempfile()
    path2 <- tempfile()

    impl$write(sdf_1, path1)
    impl$write(sdf_2, path2)

    sdf_1 <- impl$read(sc, path1)
    expect_equivalent(sdf_1 %>% collect(), tbl_1)
    sdf_2 <- impl$read(sc, path2)
    expect_equivalent(sdf_2 %>% collect(), tbl_2)
    expect_equivalent(sdf_1 %>% collect(), tbl_1)
  }
})

test_that("spark_save_table() / spark_load_table() warn and delegate", {
  skip_on_livy()
  skip_connection("format-table")
  skip_databricks_connect()
  if (spark_version(sc) < "2.0.0") {
    skip("tables not supported before 2.0.0")
  }
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(id = 1:3), overwrite = TRUE)
  tbl <- random_string("test_deprecated_table")

  # spark_save_table() delegates to spark_write_table()
  expect_warning(spark_save_table(df, tbl), "deprecated")
  expect_equal(sdf_nrow(tbl(sc, tbl)), 3)

  # spark_load_table() delegates to spark_read_table(); `path` is ignored
  expect_warning(loaded <- spark_load_table(sc, tbl, path = tbl), "deprecated")
  expect_equal(sdf_nrow(loaded), 3)
})

test_that("spark_expect_jobj_class() errors on a mismatched class", {
  skip_on_livy()
  skip_connection("format-table")

  df <- spark_dataframe(testthat_tbl("iris"))
  expect_error(
    spark_expect_jobj_class(df, "org.apache.spark.sql.NotADataFrame"),
    "only supported on"
  )
})

test_that("spark_write_jdbc() errors when the 'url' option is missing", {
  skip_on_livy()
  skip_connection("format-table")

  expect_error(
    spark_write_jdbc(testthat_tbl("iris"), name = "no_url", options = list()),
    "Option 'url' is expected"
  )
})

test_that("spark_write_parquet() accepts a list of modes", {
  skip_on_livy()
  # exercises the list-mode branch of spark_data_apply_mode()
  skip_connection("format-parquet")
  skip_databricks_connect()

  sdf <- sdf_copy_to(sc, data.frame(id = 1:3), overwrite = TRUE)
  path <- tempfile(pattern = "test_write_parquet_list_mode_")

  spark_write_parquet(sdf, path, mode = list("overwrite"))
  expect_equal(sdf_nrow(spark_read_parquet(sc, random_string(), path)), 3)
})

test_clear_cache()
