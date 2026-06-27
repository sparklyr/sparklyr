skip_connection("stream")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()
test_requires("dplyr")

sc <- testthat_spark_connection()

test_that("stream test generates file", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  expect_silent(stream_generate_test(iterations = 1))
  ss <- read.csv("source/stream_1.csv")
  expect_equal(ss$x, 1:12)
  on.exit(unlink("source", recursive = TRUE))
})

test_stream("Stream lag works", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  test_requires("dplyr")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    filter(Species == "virginica") %>%
    stream_write_csv(iris_out)

  id <- stream_id(stream)

  sf <- stream_find(sc, id)

  succeed()
})

test_stream("Stream lag works", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  test_requires("dplyr")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";")

  expect_is(
    stream_lag(
      x = stream,
      cols = c(previous = Species ~ 1)
    ) %>%
      collect(),
    "data.frame"
  )

  expect_error(
    stream_lag(
      x = stream,
      cols = "no-a-column"
    )
  )

  succeed()
})


test_stream("csv stream can be filtered with dplyr", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  test_requires("dplyr")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    filter(Species == "virginica") %>%
    stream_write_csv(iris_out)

  stream_stop(stream)

  expect_equal(substr(capture.output(stream)[1], 1, 6), "Stream")
  expect_is(stream_id(stream), "character")

  succeed()
})

test_stream("SDF collect works", {
  stream <- stream_read_csv(sc, iris_in, delimiter = ";")

  df <- sdf_collect_stream(stream)

  expect_is(df, "data.frame")

  succeed()
})

test_stream("csv stream can use custom options", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, delimiter = "|")

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from memory", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_memory("iris_stream")

  stream_stop(stream)

  succeed()
})

test_stream("stream can read and write from text", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_text(sc, iris_in) %>%
    stream_write_text(iris_out)

  stream_stop(stream)
  succeed()
})

test_stream("stream can read and write from json", {
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

test_stream("stream can read and write from parquet", {
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

test_stream("stream can read and write from orc", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_orc("orc-in")

  stream_stop(stream)
  succeed()
})

test_stream("stream_lag() works as expected", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")
  skip_on_arrow()

  weekdays_sdf <- stream_read_csv(sc, get_test_data_path("weekdays"))
  expect_true(weekdays_sdf %>% sdf_is_streaming())

  expected <- dplyr::tribble(
    ~day        , ~x , ~yesterday  , ~two_days_ago ,
    "Monday"    , 1L , NA          , NA            ,
    "Tuesday"   , 1L , "Monday"    , NA            ,
    "Wednesday" , 2L , "Tuesday"   , "Monday"      ,
    "Thursday"  , 3L , "Wednesday" , "Tuesday"     ,
    "Friday"    , 3L , "Thursday"  , "Wednesday"   ,
    "Saturday"  , 3L , "Friday"    , "Thursday"    ,
    "Sunday"    , 6L , "Saturday"  , "Friday"      ,
  )
  output_sdf <- weekdays_sdf %>%
    stream_lag(cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2))
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(output_sdf %>% collect(), expected)

  expected <- dplyr::tribble(
    ~day        , ~x , ~yesterday  , ~two_days_ago ,
    "Monday"    , 1L , NA          , NA            ,
    "Tuesday"   , 1L , "Monday"    , NA            ,
    "Wednesday" , 2L , "Tuesday"   , "Monday"      ,
    "Thursday"  , 3L , "Wednesday" , NA            ,
    "Friday"    , 3L , "Thursday"  , "Wednesday"   ,
    "Saturday"  , 3L , "Friday"    , "Thursday"    ,
    "Sunday"    , 6L , NA          , NA            ,
  )
  output_sdf <- weekdays_sdf %>%
    stream_lag(
      cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2),
      thresholds = c(x = 1L)
    )
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(output_sdf %>% collect(), expected)
  weekdays_sdf <- stream_read_csv(sc, get_test_data_path("weekdays")) %>%
    dplyr::mutate(x = dplyr::sql("CAST (`x` AS TIMESTAMP)"))
  expect_true(weekdays_sdf %>% sdf_is_streaming())
  output_sdf <- weekdays_sdf %>%
    stream_lag(
      cols = c(yesterday = day ~ 1, two_days_ago = day ~ 2),
      thresholds = c(x = "1s")
    )
  expect_true(output_sdf %>% sdf_is_streaming())
  expect_equivalent(
    output_sdf %>% collect(),
    expected %>% dplyr::mutate(x = as.POSIXct(x, origin = "1970-01-01"))
  )
})

test_stream("stream write handles partitioning columns correctly", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_csv(iris_out, partition_by = "Species")

  stream_stop(stream)

  sub_dirs <- dir(iris_out_dir)

  for (partition in c(
    "Species=setosa",
    "Species=versicolor",
    "Species=virginica"
  )) {
    expect_true(partition %in% sub_dirs)
  }
})

test_stream("Adds watermark step", {
  test_requires_version("2.0.0", "Spark streaming requires Spark 2.0 or above")

  stream <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_watermark() %>%
    stream_write_memory("iris_stream")

  stream_stop(stream)

  succeed()
})

test_stream("stream can read and write from Delta", {
  test_requires_version("3.0.0", max_version = "4")

  delta_in <- file.path(base_dir, "delta-in")

  stream_in <- stream_read_csv(sc, iris_in, delimiter = ";") %>%
    stream_write_delta(delta_in)

  stream_stop(stream_in)

  succeed()
})

test_that("to_milliseconds() responds as expected", {
  expect_equal(to_milliseconds("1 second"), 1000)
  expect_equal(to_milliseconds(1000), 1000)
  expect_error(to_milliseconds("zzz"))
  expect_error(to_milliseconds(list()))
})

test_that("to_milliseconds() converts every supported unit", {
  # milliseconds
  expect_equal(to_milliseconds("5ms"), 5)
  expect_equal(to_milliseconds("5 msec"), 5)
  expect_equal(to_milliseconds("5 msecs"), 5)
  expect_equal(to_milliseconds("5 millisecond"), 5)
  expect_equal(to_milliseconds("5 milliseconds"), 5)
  # seconds
  expect_equal(to_milliseconds("2s"), 2000)
  expect_equal(to_milliseconds("2 sec"), 2000)
  expect_equal(to_milliseconds("2 secs"), 2000)
  expect_equal(to_milliseconds("2 seconds"), 2000)
  # minutes
  expect_equal(to_milliseconds("1m"), 60000)
  expect_equal(to_milliseconds("1 min"), 60000)
  expect_equal(to_milliseconds("1 mins"), 60000)
  expect_equal(to_milliseconds("1 minute"), 60000)
  expect_equal(to_milliseconds("1 minutes"), 60000)
  # hours
  expect_equal(to_milliseconds("1h"), 3600000)
  expect_equal(to_milliseconds("1 hr"), 3600000)
  expect_equal(to_milliseconds("1 hrs"), 3600000)
  expect_equal(to_milliseconds("1 hour"), 3600000)
  expect_equal(to_milliseconds("1 hours"), 3600000)
  # days
  expect_equal(to_milliseconds("1d"), 86400000)
  expect_equal(to_milliseconds("1 day"), 86400000)
  expect_equal(to_milliseconds("1 days"), 86400000)
})

test_that("to_milliseconds() rejects unknown units and malformed strings", {
  # well-formed number + unrecognized unit
  expect_error(
    to_milliseconds("5 fortnights"),
    "not a valid time duration"
  )
  # string that does not match the <number><unit> pattern at all
  expect_error(
    to_milliseconds("abc"),
    "not a valid time duration"
  )
})

test_that("stream_trigger_interval() builds the expected structure", {
  default <- stream_trigger_interval()
  expect_s3_class(default, "stream_trigger_interval")
  expect_equal(default$interval, 1000)

  custom <- stream_trigger_interval(interval = 250)
  expect_s3_class(custom, "stream_trigger_interval")
  expect_equal(custom$interval, 250)
})

test_that("stream_trigger_continuous() builds the expected structure", {
  default <- stream_trigger_continuous()
  expect_s3_class(default, "stream_trigger_continuous")
  expect_equal(default$interval, 5000)

  custom <- stream_trigger_continuous(checkpoint = 2000)
  expect_s3_class(custom, "stream_trigger_continuous")
  expect_equal(custom$interval, 2000)
})

test_that("stream_trigger_create() dispatches per trigger class", {
  # Capture the static method requested by each trigger type.
  captured <- list()
  with_mocked_bindings(
    invoke_static = function(sc, class, method, value) {
      captured <<- list(class = class, method = method, value = value)
      "trigger"
    },
    .package = "sparklyr",
    {
      stream_trigger_create(stream_trigger_interval(interval = 750), sc = "sc")
      expect_equal(
        captured$class,
        "org.apache.spark.sql.streaming.Trigger"
      )
      expect_equal(captured$method, "ProcessingTime")
      expect_identical(captured$value, 750L)

      stream_trigger_create(
        stream_trigger_continuous(checkpoint = 1500),
        sc = "sc"
      )
      expect_equal(captured$method, "Continuous")
      expect_identical(captured$value, 1500L)
    }
  )
})

test_that("stream_generate_test() errors when 'later' is unavailable", {
  with_mocked_bindings(
    installed.packages = function(...) matrix("not-later"),
    .package = "sparklyr",
    expect_error(stream_generate_test(), "later")
  )
})

test_that("stream_generate_test() coerces input and creates the path", {
  out <- tempfile("stream_gen_")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  # A single iteration with a non-data-frame input exercises the coercion and
  # directory-creation branches and writes exactly one file synchronously.
  expect_silent(
    stream_generate_test(
      df = 1:5,
      path = out,
      distribution = 3,
      iterations = 1,
      interval = 0
    )
  )
  expect_true(dir.exists(out))
  expect_true(file.exists(file.path(out, "stream_1.csv")))
})

test_that("stream_stop() stops the stream and unregisters it", {
  # No live Spark: fake stream object carrying its own jobj id, with every
  # Spark interaction mocked. Covers the no-job branch of stream_stop() plus
  # stream_unregister() removing the stream from connection state.
  state_env <- new.env()
  state_env$streams <- new.env()
  fake_sc <- structure(list(state = state_env), class = "spark_connection")
  fake_stream <- structure(list(), class = "spark_stream")
  state_env$streams[["jobj-1"]] <- fake_stream

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_jobj_id = function(x) "jobj-1",
    invoke = function(jobj, method, ...) {
      if (method == "stop") "stopped" else NULL
    },
    .package = "sparklyr",
    {
      result <- stream_stop(fake_stream)
      expect_equal(result, "stopped")
      # stream removed from the connection's stream registry
      expect_null(state_env$streams[["jobj-1"]])
    }
  )
})

test_that("stream_unregister() returns early on NULL input", {
  expect_null(stream_unregister(NULL))
})

test_that("stream_unregister_all() unregisters each tracked stream", {
  # streams are stored as a named list on the connection state (not an env);
  # stream_unregister() removes entries with `[[id]] <- NULL`
  state_env <- new.env()
  state_env$streams <- list(
    "jobj-a" = structure(list(id = "jobj-a"), class = "spark_stream"),
    "jobj-b" = structure(list(id = "jobj-b"), class = "spark_stream")
  )
  fake_sc <- structure(list(state = state_env), class = "spark_connection")

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_jobj_id = function(x) x$id,
    .package = "sparklyr",
    {
      stream_unregister_all(fake_sc)
      expect_equal(length(state_env$streams), 0)
    }
  )
})

test_that("stream_register() stores the stream when jobs API is unavailable", {
  # When the RStudio jobs API is not available, register simply records the
  # stream in connection state and returns it unchanged.
  state_env <- new.env()
  fake_sc <- structure(
    list(state = state_env, config = list()),
    class = "spark_connection"
  )
  fake_stream <- structure(list(), class = "spark_stream")

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_jobj_id = function(x) "jobj-reg",
    rstudio_jobs_api_available = function() FALSE,
    spark_config_logical = function(config, name, default) default,
    .package = "sparklyr",
    {
      result <- stream_register(fake_stream)
      expect_identical(result, fake_stream)
      expect_identical(state_env$streams[["jobj-reg"]], fake_stream)
    }
  )
})

test_clear_cache()
