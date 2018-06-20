context("streaming")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("csv stream can be filtered with dplyr", {
  dir.create("iris-in")
  write.csv(iris, "iris-in/iris.csv")

  stream_read_csv(sc, "stream", "iris-in") %>%
    filter(stream._c3 == "virginica") %>%
    stream_write_csv("iris-out")

  succeed()
})
