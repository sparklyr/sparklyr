skip_connection("core_invoke")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("we can invoke_static with 0 arguments", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "nullary"), 0)
})

test_that("we can invoke_static with 1 scalar argument", {
  expect_equal(
    invoke_static(
      sc,
      "sparklyr.Test",
      "unaryPrimitiveInt",
      5L
    ),
    25
  )

  expect_error(invoke_static(sc, "sparklyr.Test", "unaryPrimitiveInt", NULL))

  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryInteger", 0L), TRUE)
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "unaryInteger", -2147483647L),
    FALSE
  )
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "unaryInteger", 2147483647L),
    FALSE
  )
  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryInteger", 1L), FALSE)

  # check (i == 0) evaluates to false in scala if i is null (i.e., serialization does not turn null value into 0)
  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryInteger", NULL), FALSE)
  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryInteger", NA), FALSE)

  expect_equal(
    invoke_static(
      sc,
      "sparklyr.Test",
      "unaryNullableInteger",
      5L
    ),
    25
  )

  expect_equal(
    invoke_static(sc, "sparklyr.Test", "unaryNullableInteger", NULL),
    -1
  )
})

test_that("we can invoke_static with 1 Seq argument", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "unarySeq", list(3, 4)), 25)

  expect_equal(
    invoke_static(sc, "sparklyr.Test", "unaryNullableSeq", list(3, 4)),
    25
  )
})

test_that("we can invoke_static with null Seq argument", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryNullableSeq", NULL), -1)
})

test_that("infer correct overloaded method", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "infer", 0), "Double")
  expect_equal(invoke_static(sc, "sparklyr.Test", "infer", "a"), "String")
  expect_equal(invoke_static(sc, "sparklyr.Test", "infer", list()), "Seq")
})

test_that("roundtrip date array", {
  dates <- list(as.Date("2016/1/1"), as.Date("2016/1/1"))
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "roundtrip", dates),
    do.call("c", dates)
  )
})

test_that("we can invoke_static 'package object' types", {
  expect_equal(
    invoke_static(sc, "sparklyr.test", "testPackageObject", "x"),
    "x"
  )
})

test_that("we can invoke methods with Char/Short/Long/Float parameters (#1395)", {
  expect_identical(
    invoke_new(sc, "java.lang.Character", "f"),
    "f"
  )

  expect_identical(
    invoke_new(sc, "java.lang.Short", 42L),
    42L
  )

  expect_identical(
    invoke_new(sc, "java.lang.Long", 42),
    42
  )

  expect_identical(
    invoke_new(sc, "java.lang.Long", 42L),
    42
  )

  expect_identical(
    invoke_new(sc, "java.lang.Float", 42),
    42
  )
})

test_that("numeric to Long out of range error", {
  big_number <- invoke_static(sc, "scala.Long", "MaxValue") * 2
  expect_error(
    invoke_new(sc, "java.lang.Long", big_number),
    "java\\.lang\\.Exception: Unable to cast numeric to Long: out of range\\."
  )
  expect_error(
    invoke_new(sc, "java.lang.Long", -1 * big_number),
    "java\\.lang\\.Exception: Unable to cast numeric to Long: out of range\\."
  )
})

test_that("integer to Short out of range error", {
  big_number <- invoke_static(sc, "scala.Short", "MaxValue") * 2
  expect_error(
    invoke_new(sc, "java.lang.Short", as.integer(big_number)),
    "java\\.lang\\.Exception: Unable to cast integer to Short: out of range\\."
  )
  expect_error(
    invoke_new(sc, "java.lang.Short", as.integer(-1 * big_number)),
    "java\\.lang\\.Exception: Unable to cast integer to Short: out of range\\."
  )
})

test_that("NaN is handled correctly", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "infer", NaN), "Double")
  jflt <- jfloat(sc, NaN)
  expect_equal(invoke_static(sc, "sparklyr.Test", "readFloat", jflt), NaN)
})

test_that("j_invoke variants return JVM object references", {
  # j_invoke_* force the return value to be retrieved as a JVM object reference
  obj <- j_invoke_new(sc, "java.lang.Object")
  expect_true(inherits(obj, "spark_jobj"))

  str_ref <- j_invoke(obj, "toString")
  expect_true(inherits(str_ref, "spark_jobj"))

  props <- j_invoke_static(sc, "java.lang.System", "getProperties")
  expect_true(inherits(props, "spark_jobj"))
})

# ---- Socket / dispatch helpers (no live backend needed) -------------------

test_that("core_invoke_socket picks backend or monitoring by state", {
  sc_b <- list(
    state = list(use_monitoring = FALSE),
    backend = "B",
    monitoring = "M"
  )
  expect_equal(core_invoke_socket(sc_b), "B")
  expect_equal(core_invoke_socket_name(sc_b), "backend")

  sc_m <- list(
    state = list(use_monitoring = TRUE),
    backend = "B",
    monitoring = "M"
  )
  expect_equal(core_invoke_socket(sc_m), "M")
  expect_equal(core_invoke_socket_name(sc_m), "monitoring")
})

test_that("jobj_subclass methods all resolve to shell_jobj", {
  expect_equal(jobj_subclass.shell_backend(NULL), "shell_jobj")
  expect_equal(jobj_subclass.spark_connection(NULL), "shell_jobj")
  expect_equal(jobj_subclass.spark_worker_connection(NULL), "shell_jobj")
})

test_that("invoke entry points reject a NULL connection", {
  expect_error(core_invoke_synced(NULL), "no longer valid")
  expect_error(
    core_invoke_method_impl(NULL, TRUE, FALSE, "obj", "m", FALSE),
    "no longer valid"
  )
})

# ---- Job cancellation -----------------------------------------------------

test_that("core_invoke_cancel_running short-circuits on guard conditions", {
  # no spark context
  expect_null(core_invoke_cancel_running(list(state = list())))
  # monitoring connection
  expect_null(core_invoke_cancel_running(
    list(state = list(spark_context = "x", use_monitoring = TRUE))
  ))
  # already cancelling
  expect_null(core_invoke_cancel_running(
    list(state = list(spark_context = "x", cancelling_all_jobs = TRUE))
  ))
})

test_that("core_invoke_cancel_running cancels jobs on the active context", {
  cancelled <- FALSE
  with_mocked_bindings(
    invoke = function(jobj, method, ...) {
      cancelled <<- identical(method, "cancelAllJobs")
      NULL
    },
    .package = "sparklyr",
    {
      sc_fake <- list(
        state = list(spark_context = "ctx"),
        config = list(sparklyr.progress = FALSE)
      )
      core_invoke_cancel_running(sc_fake)
    }
  )
  expect_true(cancelled)
})

# ---- Error handling + reporting -------------------------------------------

test_that("core_handle_known_errors warns on the tachyon/localhost failure", {
  expect_warning(
    core_handle_known_errors(
      list(master = "local"),
      "java.util.ServiceConfigurationError: tachyon could not be loaded"
    ),
    "validate that the hostname"
  )
})

test_that("core_handle_known_errors aborts on local worker failures", {
  sc_fake <- list(
    master = "local",
    output_file = tempfile(),
    error_file = tempfile()
  )
  with_mocked_bindings(
    abort_shell = function(...) stop("aborted by mock"),
    .package = "sparklyr",
    expect_error(
      core_handle_known_errors(sc_fake, "please check worker logs for details"),
      "aborted by mock"
    )
  )
})

test_that("core_read_spark_log_error reads logs or falls back to a default", {
  log <- tempfile()
  writeLines(c("2024-01-01 INFO starting", "2024-01-01 ERROR boom"), log)
  msg <- core_read_spark_log_error(list(output_file = log))
  expect_match(msg, "failed to invoke spark command")
  expect_match(msg, "ERROR boom")

  # a missing log file falls back to the unknown-reason message (readLines
  # warns before erroring; the try() only silences the error)
  expect_equal(
    suppressWarnings(core_read_spark_log_error(list(output_file = tempfile()))),
    "failed to invoke spark command (unknown reason)"
  )
})

test_that("spark_error honors the simple-errors option", {
  withr::local_options(sparklyr.simple.errors = TRUE)
  expect_error(spark_error("simple boom"), "simple boom")
})

test_that("spark_error formats a rich, hyperlinked message", {
  withr::local_options(sparklyr.simple.errors = NULL)

  # color terminal -> builds the OSC 8 hyperlink (x-r-run scheme, not RStudio)
  withr::local_envvar(TERM = "xterm-256color")
  expect_error(
    spark_error("boom\n\tat some.scala.Frame"),
    "see the full Spark error"
  )

  # non-color terminal -> plain backticked function reference
  withr::local_envvar(TERM = "dumb")
  expect_error(
    spark_error("boom\n\tat some.scala.Frame"),
    "see the full Spark error"
  )
})

test_that("spark_last_error reports the stored error or its absence", {
  orig <- genv_get_last_error()
  withr::defer(genv_set_last_error(orig))

  genv_set_last_error("previous failure")
  expect_message(spark_last_error(), "previous failure")

  genv_set_last_error(NULL)
  expect_message(spark_last_error(), "No error found")
})

# ---- invoke_trace ---------------------------------------------------------

test_that("invoke_trace honors the sparklyr.log.invoke setting", {
  expect_output(
    invoke_trace(
      list(config = list(sparklyr.log.invoke = "cat")),
      "Invoking",
      "m"
    ),
    "Invoking m"
  )
  expect_message(
    invoke_trace(
      list(config = list(sparklyr.log.invoke = TRUE)),
      "Invoking",
      "m"
    ),
    "Invoking m"
  )
  expect_message(
    invoke_trace(
      list(config = list(sparklyr.log.invoke = "callstack")),
      "Invoking",
      "m"
    )
  )
  # default (FALSE) emits nothing
  expect_silent(
    invoke_trace(list(config = list()), "Invoking", "m")
  )
})

test_clear_cache()
