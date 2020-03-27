context("print")
test_requires("dplyr")
sc <- testthat_spark_connection()

verify_table_src <- function(printed) {
  expected <- c(
    "# Source: spark<\\?> \\[\\?\\? x 1\\]",
    "id"
  )
  for (i in seq_along(expected)) {
    expect_gt(grep(expected[[i]], printed[[i]]), 0)
  }
}

test_that("print supports spark tables", {
  printed <- capture.output(print(sdf_len(sc, 11)))

  verify_table_src(printed)

  expect_true(grepl("with more rows", printed[14]))
})

test_that("can print more than 10 rows from a spark tables", {
  printed <- capture.output(sdf_len(sc, 100) %>% print(n = 50))

  verify_table_src(printed)

  expect_true(grepl("1", printed[4]))
  expect_true(grepl("49", printed[52]))
  expect_true(grepl("50", printed[53]))
  expect_true(grepl("with more rows", printed[54]))
})
