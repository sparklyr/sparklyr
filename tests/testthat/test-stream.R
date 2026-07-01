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
  # Capped at Spark 4.1+: no compatible delta-spark build exists yet — delta-spark
  # 4.0.0 (the latest) throws NoSuchMethodError on Spark 4.1. Works on Spark 3.x/4.0.
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

test_that("stream_register() wires up the RStudio jobs API when available", {
  # When the jobs API is available, register attaches a job handle, records
  # initial progress, and builds the info/stop job actions. We mock the jobs API
  # (its helpers live behind `# nocov` in jobs_api.R) and then invoke the
  # captured actions to cover their bodies.
  state_env <- new.env()
  fake_sc <- structure(
    list(
      state = state_env,
      config = list(),
      method = "shell",
      master = "local",
      app_name = "sparklyr"
    ),
    class = "spark_connection"
  )
  fake_stream <- structure(list(), class = "spark_stream")

  captured_actions <- NULL
  progress <- list()
  console <- NULL

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_jobj_id = function(x) "jobj-job",
    spark_config_logical = function(config, name, default) TRUE,
    rstudio_jobs_api_available = function() TRUE,
    rstudio_jobs_api = function() {
      list(
        add_job_progress = function(job, units) {
          progress[[length(progress) + 1]] <<- units
        },
        remove_job = function(job) NULL
      )
    },
    rstudio_jobs_api_new = function(name, units, actions) {
      captured_actions <<- actions
      "job-handle"
    },
    stream_id = function(x) "stream-1",
    stream_stop = function(stream) "stopped",
    sendToConsole = function(...) console <<- paste0(...),
    .package = "sparklyr",
    {
      result <- stream_register(fake_stream)
      expect_equal(result$job, "job-handle")
      expect_equal(progress[[1]], 10L)

      # exercise the captured job actions; info() searches the global env for the
      # connection, so make it findable there.
      assign("the_sc", fake_sc, envir = .GlobalEnv)
      withr::defer(rm("the_sc", envir = .GlobalEnv))
      captured_actions$info("id")
      expect_match(console, "stream_find")
      expect_equal(captured_actions$stop("id"), "stopped")
    }
  )
})

test_that("stream_stop()/stream_unregister() drive the jobs API for a job-backed stream", {
  state_env <- new.env()
  state_env$streams <- new.env()
  fake_sc <- structure(list(state = state_env), class = "spark_connection")
  fake_stream <- structure(list(job = "job-handle"), class = "spark_stream")
  state_env$streams[["jobj-j"]] <- fake_stream

  removed <- FALSE
  progress <- NULL

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_jobj_id = function(x) "jobj-j",
    rstudio_jobs_api = function() {
      list(
        add_job_progress = function(job, units) progress <<- units,
        remove_job = function(job) removed <<- TRUE
      )
    },
    invoke = function(jobj, method, ...) {
      if (method == "stop") "stopped" else NULL
    },
    .package = "sparklyr",
    {
      result <- stream_stop(fake_stream)
      expect_equal(result, "stopped")
      expect_equal(progress, 100L) # add_job_progress(job, 100L)
      expect_true(removed) # stream_unregister() -> remove_job
      expect_null(state_env$streams[["jobj-j"]])
    }
  )
})

test_that("stream_validate() returns the stream when there is no exception", {
  fake_sc <- structure(list(config = list()), class = "spark_connection")
  fake_stream <- structure(list(), class = "spark_stream")

  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_config_value = function(config, name, default) 0,
    stream_status = function(stream) list(message = "waiting for data"),
    invoke = function(jobj, method, ...) {
      if (method == "isEmpty") TRUE else "jobj"
    },
    .package = "sparklyr",
    expect_identical(stream_validate(fake_stream), fake_stream)
  )
})

test_that("stream_validate() raises the exception cause (getMessage then toString)", {
  fake_sc <- structure(list(config = list()), class = "spark_connection")
  fake_stream <- structure(list(), class = "spark_stream")

  make_invoke <- function(message_value) {
    function(jobj, method, ...) {
      switch(
        method,
        isEmpty = FALSE,
        getMessage = message_value,
        toString = "boom-tostring",
        "jobj"
      )
    }
  }

  # cause has a message -> getMessage path
  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_config_value = function(config, name, default) 0,
    stream_status = function(stream) list(message = "waiting"),
    invoke = make_invoke("boom-message"),
    .package = "sparklyr",
    expect_error(stream_validate(fake_stream), "boom-message")
  )

  # cause message is NULL -> toString fallback
  with_mocked_bindings(
    spark_connection = function(x, ...) fake_sc,
    spark_config_value = function(config, name, default) 0,
    stream_status = function(stream) list(message = "waiting"),
    invoke = make_invoke(NULL),
    .package = "sparklyr",
    expect_error(stream_validate(fake_stream), "boom-tostring")
  )
})

test_that("stream_generate_test() iterates and reschedules through later", {
  out <- withr::local_tempdir()

  # Run the scheduled work synchronously so the recursive generator drains in
  # one call. The generator fetches `later` via get(asNamespace("later")), which
  # picks up this mocked binding.
  with_mocked_bindings(
    later = function(fn, delay = 0) fn(),
    .package = "later",
    stream_generate_test(
      df = data.frame(x = 1:2),
      path = out,
      distribution = c(5, 1),
      iterations = 3,
      interval = 0
    )
  )

  expect_true(all(file.exists(file.path(out, paste0("stream_", 1:3, ".csv")))))
})

test_that("stream_lag() errors on a non-streaming dataframe", {
  mtcars_tbl <- testthat_tbl("mtcars")

  expect_error(
    stream_lag(mtcars_tbl, cols = c(prev = mpg ~ 1)),
    "expected a streaming dataframe"
  )
})

test_clear_cache()
