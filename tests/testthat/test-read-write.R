context("read-write")

sc <- testthat_spark_connection()
iris_table_name <- random_table_name("iris")

test_that("spark_read_csv() succeeds when column contains similar non-ascii", {
  if (.Platform$OS.type == "windows")
    skip("CSV encoding is slightly different in windows")
  skip_databricks_connect()

  csvPath <- get_sample_data_path(
    "spark-read-csv-column-containing-non-ascii.csv"
  )
  df <- spark_read_csv(sc,name="teste",path=csvPath,header = TRUE,
                       delimiter = ";",charset = "Latin1",memory = FALSE)

  expect_true(
    all(dplyr::tbl_vars(df) == c("Municipio", "var", "var_1_0")),
    info = "success reading non-ascii similar columns from csv")
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

  spark_write_table(df, "test_write_table_new")

  append_table <- tbl(sc, "test_write_table_new")

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
  skip_on_arrow()

  fpath <- get_sample_data_path("with_embedded_nul\\.csv$")
  df <- spark_read_csv(sc, name = "test_embedded_nul", path = fpath)
  expect_equal(
    suppressWarnings(df %>% collect()),
    data.frame(test = "teststring", stringsAsFactors = FALSE)
  )
  expect_warning(
    df %>% collect(),
    "Input contains embedded nuls, removing."
  )
})

teardown({
  db_drop_table(iris_table_name)
})
