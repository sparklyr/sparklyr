skip_connection("arrow")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

# NB: the arrow serialization round-trip (arrow_copy_to / arrow_collect /
# arrow_read_stream) requires {arrow} to be attached and is exercised by the
# dedicated arrow CI job + test-zzz-spark_apply.R. Here we cover the pure
# arrow_enabled* predicates, which decide whether that path is taken.

test_that("arrow_enabled_object predicates classify objects", {
  expect_true(arrow_enabled_object(1)) # default

  sdf <- spark_dataframe(testthat_tbl("iris"))
  expect_type(arrow_enabled_object(sdf), "logical") # spark_jobj (schema check)

  # a data frame with an unsupported list-of-raw column disables arrow + warns
  df <- dplyr::tibble(a = list(as.raw(1:2), as.raw(3:4)))
  expect_warning(enabled <- arrow_enabled_object(df), "Arrow disabled")
  expect_false(enabled)
})

test_that("arrow_enabled short-circuits when disabled via config", {
  # Deterministic regardless of whether {arrow} is attached (the arrow/coverage
  # CI jobs attach it at session init): sparklyr.arrow = FALSE makes
  # spark_config_value() return FALSE, short-circuiting arrow_enabled().
  expect_false(
    arrow_enabled(
      list(config = list(sparklyr.arrow = FALSE)),
      data.frame(a = 1)
    )
  )
})

test_that("spark_avro_package_name maps versions and rejects unsupported", {
  expect_equal(
    spark_avro_package_name("3.5.0"),
    "org.apache.spark:spark-avro_2.12:3.5.0"
  )
  expect_equal(
    spark_avro_package_name("4.0.0"),
    "org.apache.spark:spark-avro_2.13:4.0.0"
  )
  expect_equal(
    spark_avro_package_name("2.4.8"),
    "org.apache.spark:spark-avro_2.11:2.4.8"
  )
  expect_match(
    spark_avro_package_name("4.1.0-preview1"),
    "spark-avro_2.13:4.1.0-preview1"
  )
  expect_error(spark_avro_package_name(NULL), "requires Spark version")
  expect_error(spark_avro_package_name("2.3.0"), "2.4.0 or newer")
})

test_clear_cache()
