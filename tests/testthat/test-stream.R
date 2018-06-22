context("streaming")
test_requires("dplyr")
sc <- testthat_spark_connection()

dir.create("iris-in")

iris_in <- paste0("file://", file.path(getwd(), "iris-in"))
iris_out <- paste0("file://", file.path(getwd(), "iris-out"))

test_that("csv stream can be filtered with dplyr", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")
  test_requires("dplyr")

  write.csv(iris, file.path("iris-in", "iris.csv"), row.names = FALSE)

  stream <- stream_read_csv(sc, iris_in) %>%
    filter(Species == "virginica") %>%
    stream_write_csv(iris_out)

  invoke(stream, "stop")

  succeed()
})

test_that("csv stream can use custom options", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")

  write.table(iris, file.path("iris-in", "iris.csv"), row.names = FALSE, sep = ";")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, delimiter = "|")

  invoke(stream, "stop")

  succeed()
})
