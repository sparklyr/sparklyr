# Unit tests for non-Spark helper logic in R/data_interface.R.
# These exercise pure-R validation paths and need no Spark connection.

test_that("spark_read_compat_param() requires a path", {
  # data_interface.R:17-18 — both name and path NULL.
  # `sc` is intentionally NULL here: this branch errors before `sc` is ever
  # used (only the gen_sdf_name() branches read sc$config).
  expect_error(
    spark_read_compat_param(NULL, name = NULL, path = NULL),
    "The 'path' parameter must be specified"
  )
})

test_that("spark_read_compat_param() resolves name/path combinations", {
  sc <- list(config = list()) # gen_sdf_name() only reads sc$config

  # name == path: treat the value as the path, generate a name
  out <- spark_read_compat_param(sc, name = "data.csv", path = "data.csv")
  expect_length(out, 2)
  expect_identical(out[[2]], "data.csv")

  # only path supplied: generate a name from the path
  out <- spark_read_compat_param(sc, name = NULL, path = "data.csv")
  expect_identical(out[[2]], "data.csv")

  # both supplied: passed through unchanged
  expect_identical(
    spark_read_compat_param(sc, name = "tbl", path = "data.csv"),
    c("tbl", "data.csv")
  )
})

test_that("spark_data_apply_mode() errors on an unsupported mode type", {
  # data_interface.R:527 — numeric mode hits neither the list nor character branch
  expect_error(
    spark_data_apply_mode(list(), 123),
    "Unsupported type"
  )
})

test_that("spark_data_apply_mode() returns options unchanged for NULL mode", {
  # data_interface.R:521 — the is.null(mode) short-circuit
  opts <- list(x = 1)
  expect_identical(spark_data_apply_mode(opts, NULL), opts)
})

test_that("avro_set_schema() rejects a non-character schema", {
  # data_interface.R:1285
  expect_error(
    avro_set_schema(list(), 123),
    "Expect Avro schema to be a JSON string"
  )
})

test_that("avro_set_schema() attaches a character schema and passes through NULL", {
  # data_interface.R:1284-1290
  expect_identical(avro_set_schema(list(), NULL), list())
  expect_identical(
    avro_set_schema(list(), "{\"type\":\"record\"}")$avroSchema,
    "{\"type\":\"record\"}"
  )
})
