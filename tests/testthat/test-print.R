context("print")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("print supports spark tables", {
  printed <- capture.output(print(sdf_len(sc, 2)))

  expect_equal(
    printed,
    c(
      "# Source: spark<?> [?? x 1]",
      "     id",
      "* <dbl>",
      "1     1",
      "2     2"
    )
  )
})
