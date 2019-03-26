context("read-write")

sc <- testthat_spark_connection()

test_that("spark_read_csv() succeeds when column contains similar non-ascii", {
  if (.Platform$OS.type == "windows")
    skip("CSV encoding is slightly different in windows")

  csv <- file("test.csv", "w+", encoding = "latin1")
  cat("Município;var;var 1.0\n1233;1;2", file=csv)
  close(csv)

  df <- spark_read_csv(sc,name="teste",path="test.csv",header = TRUE,
                       delimiter = ";",charset = "Latin1",memory = FALSE)

  expect_true(
    all(dplyr::tbl_vars(df) == c("Municipio", "var", "var_1_0")),
    info = "success reading non-ascii similar columns from csv")

  file.remove("test.csv")
})

test_that("spark_read_json() can load data using column names", {
  json <- file("test.json", "w+")
  cat("{\"Sepal_Length\":5.1,\"Species\":\"setosa\", \"Other\": { \"a\": 1, \"b\": \"x\"}}\n", file = json)
  cat("{\"Sepal_Length\":4.9,\"Species\":\"setosa\", \"Other\": { \"a\": 2, \"b\": \"y\"}}\n", file = json)
  close(json)

  df <- spark_read_json(
    sc,
    name = "iris_json_named",
    path = "test.json",
    columns = c("a", "b", "c")
  )

  testthat::expect_equal(colnames(df), c("a", "b", "c"))

  file.remove("test.json")
})

test_that("spark_read_json() can load data using column types", {
  test_requires("dplyr")

  json <- file("test.json", "w+")
  cat("{\"Sepal_Length\":5.1,\"Species\":\"setosa\", \"Other\": { \"a\": 1, \"b\": \"x\"}}\n", file = json)
  cat("{\"Sepal_Length\":4.9,\"Species\":\"setosa\", \"Other\": { \"a\": 2, \"b\": \"y\"}}\n", file = json)
  close(json)

  df <- spark_read_json(
    sc,
    name = "iris_json_typed",
    path = "test.json",
    columns = list("Sepal_Length" = "character", "Species" = "character", "Other" = "struct<a:integer,b:character>")
  ) %>% collect()

  expect_true(is.character(df$Sepal_Length))
  expect_true(is.character(df$Species))
  expect_true(is.list(df$Other))

  file.remove("test.json")
})

test_that("spark_read_csv() can read long decimals", {
  csv <- file("test.csv", "w+")
  cat("decimal\n1\n12312312312312300000000000000", file = csv)
  close(csv)

  df <- spark_read_csv(
    sc,
    name = "test_big_decimals",
    path = "test.csv"
  )

  expect_equal(sdf_nrow(df), 2)

  file.remove("test.csv")
})

test_that("spark_read_text() and spark_write_text() read and write basic files", {
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

  spark_write_table(iris_tbl, "iris_append_tbl")
  expect_equal(
    sdf_nrow(tbl(sc, "iris_append_tbl")),
    nrow(iris)
  )

  spark_write_table(iris_tbl, "iris_append_tbl", mode = "append")
  expect_equal(
    sdf_nrow(tbl(sc, "iris_append_tbl")),
    2 * nrow(iris)
  )
})

test_that("spark_write_table() can write data", {
  if (spark_version(sc) < "2.0.0") skip("tables not supported before 2.0.0")
  test_requires("dplyr")

  df <- copy_to(sc, data.frame(id = 1L))

  spark_write_table(df, "test_write_table_new")

  append_table <- tbl(sc, "test_write_table_new")

  expect_equal(sdf_nrow(append_table), 1)
})

test_that("spark_read_csv() can rename columns", {
  csv <- file("test.csv", "w+")
  cat("a,b,c\n1,2,3", file = csv)
  close(csv)

  df <- spark_read_csv(
    sc,
    name = "test_column_rename",
    path = "test.csv",
    columns = c("AA", "BB", "CC")
  ) %>% collect()

  expect_equal(names(df), c("AA", "BB", "CC"))
})

test_that("spark_read_text() can read a whole file", {
  test_requires("dplyr")

  text_file <- file("test.txt", "w+")
  cat("1\n2\n3", file = text_file)
  close(text_file)

  whole_tbl <- spark_read_text(sc, "whole", normalizePath("test.txt"), overwrite = T, whole = TRUE)

  expect_equal(
    whole_tbl %>% collect() %>% nrow(),
    1L
  )
})
