skip_connection("core_jobj")
skip_on_livy()
skip_on_arrow_devel()
test_requires("dplyr")
sc <- testthat_spark_connection()

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

test_that("spark_jobj helpers: id accessor, default error, passthrough", {
  expect_equal(spark_jobj_id(list(id = "7")), "7")
  expect_error(spark_jobj(42), "Unable to retrieve")
  fake <- structure(list(id = "x"), class = "spark_jobj")
  expect_identical(spark_jobj(fake), fake)
})

test_that("jobj_create rejects non-character ids; jobj_info rejects non-jobj", {
  expect_error(jobj_create(sc, 42), "must be a character")
  expect_error(jobj_info(42), "non-jobj")
})

test_that("attach_connection recurses into environments", {
  out <- attach_connection(as.environment(list(a = 1, b = 2)), sc)
  expect_true(is.list(out))
})

test_that("jobj_inspect prints fields and methods of a live jobj", {
  out <- capture.output(suppressWarnings(jobj_inspect(spark_context(sc))))
  expect_true(any(grepl("Fields:|Methods:", out)))
})

test_clear_cache()
