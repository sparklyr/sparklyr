context("read-write-multiple")

sc <- testthat_spark_connection()

test_readwrite <- function(sc, writer, reader, name = "testtable", ...) {
  path <- file.path(dirname(sc$output_file), c("batch_1", "batch_2"))
  path_glob <- file.path(dirname(sc$output_file), "batch*")
  on.exit(unlink(path, recursive = TRUE, force = TRUE), add = TRUE)

  writer(sdf_copy_to(sc, data.frame(line = as.character(1L:3L))), path[1L])
  writer(sdf_copy_to(sc, data.frame(line = as.character(4L:6L))), path[2L])

  if (is.element("whole", names(list(...))) && isTRUE(list(...)$whole)) {
    res_1 <- reader(sc, name, path[1L], ...) %>% collect() %>% pull(contents) %>% strsplit("\n") %>% unlist() %>% sort()
    res_2 <- reader(sc, name, path[2L], ...) %>% collect() %>% pull(contents) %>% strsplit("\n") %>% unlist() %>% sort()
    res_3 <- reader(sc, name, path, ...) %>% collect() %>% pull(contents) %>% strsplit("\n") %>% unlist() %>% sort()
    res_4 <- reader(sc, name, path_glob, ...) %>% collect() %>% pull(contents) %>% strsplit("\n") %>% unlist() %>% sort()
  } else {
    res_1 <- reader(sc, name, path[1L], ...) %>% collect() %>% pull(line) %>% sort()
    res_2 <- reader(sc, name, path[2L], ...) %>% collect() %>% pull(line) %>% sort()
    res_3 <- reader(sc, name, path, ...) %>% collect() %>% pull(line) %>% sort()
    res_4 <- reader(sc, name, path_glob, ...) %>% collect() %>% pull(line) %>% sort()
  }

  list(
    all(res_1 == as.character(1:3)),
    all(res_2 == as.character(4:6)),
    all(res_3 == as.character(1:6)),
    all(res_4 == as.character(1:6))
  )
}

test_that(
  "spark_read_parquet() reads multiple parquet files",
    expect_equal(
      test_readwrite(sc = sc, writer = spark_write_parquet, reader = spark_read_parquet),
      list(TRUE, TRUE, TRUE, TRUE)
    )
)

test_that(
  "spark_read_orc() reads multiple orc files", {
    test_requires_version("2.0.0")
    expect_equal(
      test_readwrite(sc = sc, writer = spark_write_orc, reader = spark_read_orc),
      list(TRUE, TRUE, TRUE, TRUE)
    )
  }
)

test_that(
  "spark_read_json() reads multiple json files",
    expect_equal(
      test_readwrite(sc = sc, writer = spark_write_json, reader = spark_read_json),
      list(TRUE, TRUE, TRUE, TRUE)
    )
)

test_that(
  "spark_read_text() reads multiple text files",
  expect_equal(
      test_readwrite(sc = sc, writer = spark_write_text, reader = spark_read_text),
      list(TRUE, TRUE, TRUE, TRUE)
    )
)

test_that(
  "spark_read_text() throws a useful error for multiple files with whole=TRUE",
  expect_error(
    test_readwrite(sc = sc, writer = spark_write_text, reader = spark_read_text, whole = TRUE),
    "spark_read_text is only suppored with path of length 1 if whole=TRUE"
  )
)
