skip_connection("print")
skip_on_livy()
skip_on_arrow_devel()
test_requires("dplyr")
sc <- testthat_spark_connection()

expect_regexes <- function(printed, regexes) {
  for (i in seq_along(regexes)) {
    expect_gt(grep(regexes[[i]], printed[[i]]), 0)
  }
}

verify_table_src <- function(printed) {
  expect_output(
    printed,
    regexp =  "# Source:   table<*\\>")
}

test_that("print supports spark tables", {
  verify_table_src(print(sdf_len(sc, 11)))
})

test_that("can print more than 10 rows from a spark tables", {
  test_print <- sdf_len(sc, 100)

  verify_table_src(print(test_print, n = 50))

  printed <- capture.output(print(test_print, n = 50))

  expect_true(grepl("1", printed[5]))
  expect_true(grepl("49", printed[53]))
  expect_true(grepl("50", printed[54]))
  expect_true(grepl("more rows", printed[55]))
})

test_that("print supports spark context", {
  printed <- capture.output(print(spark_context(sc)))
  expect_regexes(
    printed,
    c(
      "<jobj\\[\\d+\\]>",
      "org\\.apache\\.spark\\.SparkContext",
      "org\\.apache\\.spark\\.SparkContext@[0-9a-f]+",
      "^$",
      "appName:\\s+",
      "master:\\s+",
      "files:\\s+",
      "jars:\\s+"
    )
  )
})

test_clear_cache()

