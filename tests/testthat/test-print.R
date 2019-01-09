context("print")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("print supports spark tables", {
  printed <- capture.output(print(sdf_len(sc, 11)))

  expect_equal(
    printed[1:2],
    c(
      "# Source: spark<?> [?? x 1]",
      "      id"
    )
  )

  expect_equal(
    printed[14],
    c(
      "# … with more rows"
    )
  )
})
