context("streaming")
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_in <- paste0("file:///", file.path(getwd(), "iris-in"))
iris_out <- paste0("file:///", file.path(getwd(), "iris-out"))

test_stream <- function(description, test) {
  if (!dir.exists("iris-in")) dir.create("iris-in")
  if (dir.exists("iris-out")) unlink("iris-out", recursive = TRUE)

  write.table(iris, file.path("iris-in", "iris.csv"), row.names = FALSE, sep = ";")

  on.exit(unlink("iris-", recursive = TRUE))

  test_that(description, test)
}

test_stream("csv stream can be filtered with dplyr", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")
  test_requires("dplyr")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    filter(Species == "virginica") %>%
    stream_write_csv(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("csv stream can use custom options", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from memory", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_memory("iris_stream")

  stream_stop(stream)

  succeed()
})

test_stream("stream can read and write from text", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream <- stream_read_text(sc, iris_in) %>%
    stream_write_text(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from json", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_json("json-in")

  stream_out <- stream_read_json(sc, "json-in") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream_in)
  stream_stop(stream_out)

  succeed()
})

test_stream("stream can read and write from parquet", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_parquet("parquet-in")

  stream_out <- stream_read_parquet(sc, "parquet-in") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream_in)
  stream_stop(stream_out)

  succeed()
})

test_stream("stream can read and write from orc", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_orc("orc-in")

  stream_stop(stream)
  succeed()
})
