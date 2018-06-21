context("streaming")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("csv stream can be filtered with dplyr", {
  if (spark_version(sc) < "2.0.0") skip("streams not supported before 2.0.0")
  test_requires("dplyr")

  dir.create("iris-in")
  write.csv(iris, "iris-in/iris.csv", row.names = FALSE)

  stream_read_csv(sc, "stream", "iris-in") %>%
    filter(Species == "virginica") %>%
    stream_write_csv("iris-out")

  succeed()
})
