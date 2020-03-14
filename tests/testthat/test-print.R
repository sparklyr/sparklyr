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

  expect_true(grepl("with more rows", printed[14]))
})

test_that("can print more than 10 rows from a spark tables", {
  printed <- capture.output(sdf_len(sc, 100) %>% print(n = 50))

  expect_equal(
    printed[1:2],
    c(
      "# Source: spark<?> [?? x 1]",
      "      id"
    )
  )

  expect_true(grepl("1", printed[4]))
  expect_true(grepl("49", printed[52]))
  expect_true(grepl("50", printed[53]))
  expect_true(grepl("with more rows", printed[54]))
})
