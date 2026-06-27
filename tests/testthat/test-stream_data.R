skip_connection("stream_data")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()
test_requires("dplyr")

sc <- testthat_spark_connection()

test_stream("csv stream round-trips through read and write", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out)

  stream_stop(stream)
  expect_is(stream_id(stream), "character")
})

test_stream("stream_read_csv honors an explicit column schema", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  # passing a named character vector exercises spark_data_build_types() in
  # stream_read_generic() instead of the schema-inference path
  stream <- stream_read_csv(
    sc,
    get_test_data_path("weekdays"),
    columns = c(day = "character", x = "integer")
  )

  expect_true(sdf_is_streaming(spark_dataframe(stream)))
  expect_setequal(colnames(stream), c("day", "x"))
})

test_stream("stream_read_csv accepts a Spark schema object as columns", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  # a spark_jobj passed as `columns` is used verbatim as the schema, exercising
  # the `"spark_jobj" %in% class(columns)` branch of stream_read_generic()
  schema <- stream_read_csv(sc, get_test_data_path("weekdays")) %>%
    spark_dataframe() %>%
    invoke("schema")

  stream <- stream_read_csv(
    sc,
    get_test_data_path("weekdays"),
    columns = schema
  )

  expect_true(sdf_is_streaming(spark_dataframe(stream)))
  expect_setequal(colnames(stream), c("day", "x"))
})

test_stream("text stream round-trips through read and write", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_text(sc, iris_in) %>%
    stream_write_text(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("json stream round-trips through read and write", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  json_in <- file.path(base_dir, "json-in")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_json(json_in)

  stream_out <- stream_read_json(sc, json_in) %>%
    stream_write_csv(iris_out)

  stream_stop(stream_in)
  stream_stop(stream_out)
  succeed()
})

test_stream("parquet stream round-trips through read and write", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  parquet_in <- file.path(base_dir, "parquet-in")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_parquet(parquet_in)

  stream_out <- stream_read_parquet(sc, parquet_in) %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream_in)
  stream_stop(stream_out)
  succeed()
})

test_stream("orc stream round-trips through read and write", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  orc_in <- file.path(base_dir, "orc-in")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_orc(orc_in)

  stream_out <- stream_read_orc(sc, orc_in) %>%
    stream_write_csv(iris_out)

  stream_stop(stream_in)
  stream_stop(stream_out)
  succeed()
})

test_stream("stream_write_console writes a stream to the console", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_console()

  stream_stop(stream)
  succeed()
})

test_stream("stream_write_memory accepts an explicit output mode", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_memory("iris_explicit", mode = "append")

  stream_stop(stream)
  succeed()
})

test_stream("stream_write_memory infers the output mode when omitted", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  # omitting `mode` exercises the UnsupportedOperationChecker branch that
  # auto-selects between "append" and "complete"
  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_memory("iris_inferred")

  stream_stop(stream)
  succeed()
})

test_stream("stream write partitions output by the given columns", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, partition_by = "Species")

  stream_stop(stream)

  sub_dirs <- dir(iris_out_dir)
  expect_true("Species=setosa" %in% sub_dirs)
})

test_stream("stream_write_*() rejects a non-streaming DataFrame", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  static <- sdf_len(sc, 10)

  expect_error(
    stream_write_csv(static, iris_out),
    "requires streaming context"
  )
})

test_clear_cache()
