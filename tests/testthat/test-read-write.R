context("read-write")

sc <- testthat_spark_connection()
iris_table_name <- random_table_name("iris")

test_that("spark_read_csv() succeeds when column contains similar non-ascii", {
  if (.Platform$OS.type == "windows") {
    skip("CSV encoding is slightly different in windows")
  }
  skip_databricks_connect()

  csvPath <- get_sample_data_path(
    "spark-read-csv-column-containing-non-ascii.csv"
  )
  df <- spark_read_csv(sc,
    name = "teste", path = csvPath, header = TRUE,
    delimiter = ";", charset = "Latin1", memory = FALSE
  )

  expect_true(
    all(dplyr::tbl_vars(df) == c("Municipio", "var", "var_1_0")),
    info = "success reading non-ascii similar columns from csv"
  )
})

test_that("spark_read_json() can load data using column names", {
  jsonPath <- get_sample_data_path(
    "spark-read-json-can-load-data-using-column-names.json"
  )
  df <- spark_read_json(
    sc,
    name = "iris_json_named",
    path = jsonPath,
    columns = c("a", "b", "c")
  )

  testthat::expect_equal(colnames(df), c("a", "b", "c"))
})

test_that("spark_read_json() can load data using column types", {
  test_requires("dplyr")

  jsonPath <- get_sample_data_path(
    "spark-read-json-can-load-data-using-column-types.json"
  )
  df <- spark_read_json(
    sc,
    name = "iris_json_typed",
    path = jsonPath,
    columns = list("Sepal_Length" = "character", "Species" = "character", "Other" = "struct<a:integer,b:character>")
  ) %>% collect()

  expect_true(is.character(df$Sepal_Length))
  expect_true(is.character(df$Species))
  expect_true(is.list(df$Other))
})

test_that("spark_read_csv() can read long decimals", {
  csvPath <- get_sample_data_path(
    "spark-read-csv-can-read-long-decimals.csv"
  )
  df <- spark_read_csv(
    sc,
    name = "test_big_decimals",
    path = csvPath
  )

  expect_equal(sdf_nrow(df), 2)
})

test_that("spark_read_text() and spark_write_text() read and write basic files", {
  skip_databricks_connect()
  test_requires("dplyr")

  text_file <- file("test.txt", "w+")
  cat("1\n2\n3", file = text_file)
  close(text_file)

  sdf <- spark_read_text(
    sc,
    name = "test_spark_read",
    path = "test.txt"
  )

  output_file <- "test_roundtrip.txt"
  spark_write_text(
    sdf,
    path = output_file
  )

  sdf_roundtrip <- spark_read_text(
    sc,
    name = "test_spark_roundtrip",
    path = "test_roundtrip.txt"
  )

  expect_equal(sdf %>% collect(), sdf_roundtrip %>% collect())

  file.remove("test.txt")
  unlink(output_file, recursive = TRUE)
})

test_that("spark_write_table() can append data", {
  skip_databricks_connect()
  if (spark_version(sc) < "2.0.0") skip("tables not supported before 2.0.0")
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
  skip_databricks_connect()
  if (spark_version(sc) < "2.0.0") skip("tables not supported before 2.0.0")
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(id = 1L))

  tbl <- random_string("test_write_table_new")
  spark_write_table(df, tbl)

  append_table <- tbl(sc, tbl)

  expect_equal(sdf_nrow(append_table), 1)
})

test_that("spark_read_csv() can rename columns", {
  csvPath <- get_sample_data_path(
    "spark-read-csv-can-rename-columns.csv"
  )
  df <- spark_read_csv(
    sc,
    name = "test_column_rename",
    path = csvPath,
    columns = c("AA", "BB", "CC")
  ) %>% collect()

  expect_equal(names(df), c("AA", "BB", "CC"))
})

test_that("spark_read_text() can read a whole file", {
  skip_databricks_connect()
  test_requires("dplyr")

  whole_tbl <- spark_read_text(
    sc,
    "whole",
    get_sample_data_path("spark-read-text-can-read-a-whole-file.txt"),
    overwrite = T,
    whole = TRUE
  )

  expect_equal(
    whole_tbl %>% collect() %>% nrow(),
    1L
  )
})

test_that("spark_read_csv() can read with no name", {
  test_requires("dplyr")

  csvPath <- get_sample_data_path(
    "spark-read-csv-can-read-with-no-name.txt"
  )
  expect_equal(colnames(spark_read_csv(sc, csvPath)), "a")
  expect_equal(colnames(spark_read_csv(sc, "test", csvPath)), "a")
  expect_equal(colnames(spark_read_csv(sc, path = csvPath)), "a")
})

test_that("spark_read_csv() can read with named character name", {
  test_requires("dplyr")

  csvPath <- get_sample_data_path(
    "spark-read-csv-can-read-with-named-character-name.txt"
  )

  name <- c(name = "testname")
  expect_equal(colnames(spark_read_csv(sc, name, csvPath)), "a")
})


test_that("spark_read_csv() can read column types", {
  csvPath <- get_sample_data_path(
    "spark-read-csv-can-read-column-types.txt"
  )
  df_schema <- spark_read_csv(
    sc,
    name = "test_columns",
    path = csvPath,
    columns = c(a = "byte", b = "integer", c = "double")
  ) %>%
    sdf_schema()

  expect_equal(
    df_schema,
    list(
      a = list(name = "a", type = "ByteType"),
      b = list(name = "b", type = "IntegerType"),
      c = list(name = "c", type = "DoubleType")
    )
  )
})

test_that("spark_read_csv() can read verbatim column types", {
  csvPath <- get_sample_data_path(
    "spark-read-csv-can-read-verbatim-column-types.csv"
  )
  df_schema <- spark_read_csv(
    sc,
    name = "test_columns",
    path = csvPath,
    columns = c(a = "ByteType", b = "IntegerType", c = "DoubleType")
  ) %>%
    sdf_schema()

  expect_equal(
    df_schema,
    list(
      a = list(name = "a", type = "ByteType"),
      b = list(name = "b", type = "IntegerType"),
      c = list(name = "c", type = "DoubleType")
    )
  )
})

test_that("spark_read_csv() can read if embedded nuls present", {
  skip_on_arrow() # ARROW-6582

  fpath <- get_sample_data_path("with_embedded_nul.csv")
  df <- spark_read_csv(sc, name = "test_embedded_nul", path = fpath)
  expect_equivalent(
    suppressWarnings(df %>% collect()),
    data.frame(test = "teststring", stringsAsFactors = FALSE)
  )
  expect_warning(
    df %>% collect(),
    "Input contains embedded nuls, removing."
  )
})

test_that("spark_read() works as expected", {
  paths <- c(
    "hdfs://localhost:9000/1/a/b",
    "hdfs://localhost:9000/2",
    "hdfs://localhost:9000/c/d/e",
    "hdfs://localhost:9000/f",
    "hdfs://localhost:9000/3/4/5"
  )
  reader <- function(path) {
    data.frame(md5 = digest::digest(path, algo = "md5"), length = nchar(path))
  }

  expected_md5s <- sapply(
    paths, function(x) digest::digest(x, algo = "md5")
  )
  names(expected_md5s) <- NULL

  expected_lengths <- sapply(paths, function(x) nchar(x))
  names(expected_lengths) <- NULL

  for (columns in list(
    c("md5", "length"),
    list("md5", "length"),
    list(md5 = "character", length = "integer")
  )) {
    sdf <- spark_read(
      sc,
      paths,
      reader,
      packages = c("digest"),
      columns = c("md5", "length")
    )
    expect_equivalent(
      sdf_collect(sdf),
      data.frame(
        md5 = expected_md5s,
        length = expected_lengths,
        stringsAsFactors = FALSE
      )
    )
  }
})

test_that("spark_write() works as expected", {
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
    colnames(expected) <- lapply(colnames(expected), function(x) gsub("\\.", "_", x))

    expect_equal(colnames(actual), colnames(expected))
    for (col in colnames(actual)) {
      expect_equal(actual[[col]], expected[[col]])
    }

    expect_equal(lapply(res, function(e) e$path), as.list(expected_paths))
  }

  multiple_paths <- lapply(seq(5), function(x) paste0("hdfs://file_", x))
  single_path <- "hdfs://iris"

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
})

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

test_that("spark_read_avro() works as expected", {
  test_requires_version("2.4.0", "spark_read_avro() requires Spark 2.4+")
  skip_databricks_connect()

  expected <- tibble::tibble(
    a = c(1, NaN, 3, 4, NaN),
    b = c(-2L, 0L, 1L, 3L, 6L),
    c = c("ab", "cde", "zzzz", "", "fghi")
  )

  for (avro_schema in list(NULL, test_avro_schema)) {
    actual <- spark_read_avro(
      sc,
      path = get_sample_data_path("test_spark_read.avro"),
      avro_schema = avro_schema
    ) %>%
      sdf_collect()

    expect_equal(colnames(expected), colnames(actual))

    for (col in colnames(expected)) {
      expect_equal(expected[[col]], actual[[col]])
    }
  }
})

test_that("spark_write_avro() works as expected", {
  test_requires_version("2.4.0", "spark_write_avro() requires Spark 2.4+")
  skip_databricks_connect()

  df <- tibble::tibble(
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
  test_requires_version("2.4.0")

  tbl_1 <- tibble::tibble(name = c("foo_1", "bar_1"))
  tbl_2 <- tibble::tibble(name = c("foo_2", "bar_2"))
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
    impls <- append(impls, list(structure(c(read = spark_read_avro, write = spark_write_avro))))
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

teardown({
  db_drop_table(iris_table_name)
})
