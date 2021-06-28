context("streaming")

skip_databricks_connect()
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_in <- paste0("file://", file.path(getwd(), "iris-in"))
iris_out_dir <- file.path(getwd(), "iris-out")
iris_out <- paste0("file://", iris_out_dir)

test_stream <- function(description, test) {
  if (!dir.exists("iris-in")) dir.create("iris-in")
  if (dir.exists("iris-out")) unlink("iris-out", recursive = TRUE)

  write.table(iris, file.path("iris-in", "iris.csv"), row.names = FALSE, sep = ";")

  on.exit(unlink("iris-", recursive = TRUE))

  test_that(description, test)
}

test_stream("csv stream can be filtered with dplyr", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  test_requires("dplyr")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    filter(Species == "virginica") %>%
    stream_write_csv(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("csv stream can use custom options", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from memory", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_memory("iris_stream")

  stream_stop(stream)

  succeed()
})

test_stream("stream can read and write from text", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_text(sc, iris_in) %>%
    stream_write_text(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from json", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_json("json-in")

  stream_out <- stream_read_json(sc, "json-in") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream_in)
  stream_stop(stream_out)

  succeed()
})

test_stream("stream can read and write from parquet", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_parquet("parquet-in")

  stream_out <- stream_read_parquet(sc, "parquet-in") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream_in)
  stream_stop(stream_out)

  succeed()
})

test_stream("stream can read and write from orc", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_orc("orc-in")

  stream_stop(stream)
  succeed()
})

test_stream("stream_lag() works as expected", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  skip_on_arrow()

  weekdays_sdf <- stream_read_csv(sc, get_test_data_path("weekdays"))
  expect_true(weekdays_sdf %>% sdf_is_streaming())

  expected <- tibble::tribble(
    ~day,         ~x,   ~yesterday, ~two_days_ago,
    "Monday",     1L,           NA,            NA,
    "Tuesday",    1L,     "Monday",            NA,
    "Wednesday",  2L,    "Tuesday",      "Monday",
    "Thursday",   3L,  "Wednesday",     "Tuesday",
    "Friday",     3L,   "Thursday",   "Wednesday",
    "Saturday",   3L,     "Friday",    "Thursday",
    "Sunday",     6L,   "Saturday",      "Friday",
  )
  output_sdf <- weekdays_sdf %>%
    stream_lag(cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2))
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(output_sdf %>% collect(), expected)

  expected <- tibble::tribble(
    ~day,         ~x,   ~yesterday, ~two_days_ago,
    "Monday",     1L,           NA,            NA,
    "Tuesday",    1L,     "Monday",            NA,
    "Wednesday",  2L,    "Tuesday",      "Monday",
    "Thursday",   3L,  "Wednesday",            NA,
    "Friday",     3L,   "Thursday",   "Wednesday",
    "Saturday",   3L,     "Friday",    "Thursday",
    "Sunday",     6L,           NA,            NA,
  )
  output_sdf <- weekdays_sdf %>%
    stream_lag(
      cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2),
      thresholds = c(x = 1L)
    )
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(output_sdf %>% collect(), expected)
  weekdays_sdf <- stream_read_csv(sc, get_test_data_path("weekdays")) %>%
    dplyr::mutate(x = dplyr::sql("CAST (`x` AS TIMESTAMP)"))
  expect_true(weekdays_sdf %>% sdf_is_streaming())
  output_sdf <- weekdays_sdf %>%
    stream_lag(
      cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2),
      thresholds = c(x = "1s")
    )
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(
    output_sdf %>% collect(),
    expected %>% dplyr::mutate(x = as.POSIXct(x, origin = "1970-01-01"))
  )
})

test_stream("stream write handles partitioning columns correctly", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, partition_by = "Species")

  stream_stop(stream)

  sub_dirs <- dir(iris_out_dir)

  for (partition in c("Species=setosa", "Species=versicolor", "Species=virginica")) {
    expect_true(partition %in% sub_dirs)
  }
})
