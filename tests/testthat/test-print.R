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
  expect_regexes(
    printed,
    c("# Source: spark<\\?> \\[\\?\\? x 1\\]", "id")
  )
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
