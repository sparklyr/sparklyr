skip_connection("core_utils")
skip_on_livy()
skip_on_arrow_devel()

test_that("'validate_java_version_line' fails with 1.6", {
  expect_error(
    validate_java_version_line(
      "local",
      c(
        "java version \"1.6.0\"",
        "Java(TM) SE Runtime Environment (build 1.6.0_000-000)",
        "Java HotSpot(TM) 64-Bit Server VM (build 00.000-000, mixed mode)"
      )
    )
  )
})

test_that("'validate_java_version_line' works with 1.8", {
  validate_java_version_line(
    "local",
    c(
      "java version \"1.7.0\"",
      "Java(TM) SE Runtime Environment (build 1.7.0_000-000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 00.000-000, mixed mode)"
    )
  )

  succeed()
})

test_that("'validate_java_version_line' fails with java 9 in local mode", {
  expect_error(
    validate_java_version_line(
      "local",
      c(
        "java version 9",
        "Java(TM) SE Runtime Environment (build 1.9.0_000-000)",
        "Java HotSpot(TM) 64-Bit Server VM (build 25.144-000, mixed mode)"
      )
    )
  )
})

test_that("'validate_java_version_line' works with java 9 in non-local mode", {
  validate_java_version_line(
    "yarn-client",
    c(
      "java version 9",
      "Java(TM) SE Runtime Environment (build 1.9.0_000-000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 25.144-000, mixed mode)"
    )
  )

  succeed()
})

# Since adoption of JEP 223, some Java versions are released only with Major
# version and no Minor.Security information. In such cases version is shown as
# X instead of X.0.0
# Also newer versions include a date as part of the version string. Must be able
# to differentiate numbers from dates from numbers from release.
test_that("'validate_java_version_line' works on version without minor.security and date exists", {
  validate_java_version_line(
    "local",
    c(
      "java version \"11\" 2018-09-25",
      "Java(TM) SE Runtime Environment (build 00+00-0000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 00+00-0000, mixed mode, sharing)"
    )
  )

  succeed()
})

test_that("'validate_java_version_line' works when date is present and version has x.y.z format", {
  validate_java_version_line(
    "local",
    c(
      "java version \"15.0.2\" 2021-01-19",
      "Java(TM) SE Runtime Environment (build 15.0.2+7-27)",
      "Java HotSpot(TM) 64-Bit Server VM (build 15.0.2+7-27, mixed mode, sharing)"
    )
  )

  succeed()
})

test_that("core_get_package_function resolves a function or returns NULL", {
  expect_true(is.function(core_get_package_function("stats", "sd")))
  expect_null(core_get_package_function("nosuchpkg123", "x"))
  expect_null(core_get_package_function("stats", "nosuchfn123"))
})

test_that("arrow record-batch helpers round-trip a data frame", {
  skip_if_not_installed("arrow")
  # include a tz-less POSIXt column + a Spark < 3.0 version to hit the legacy
  # IPC-format env var and the timezone-defaulting branch
  df_in <- data.frame(
    x = 1:3,
    y = c("a", "b", "c"),
    t = as.POSIXct(c("2020-01-01", "2020-01-02", "2020-01-03"))
  )
  attr(df_in$t, "tzone") <- NULL # so the timezone-defaulting branch runs
  raw <- arrow_write_record_batch(df_in, spark_version_number = "2.4.0")
  reader <- arrow_record_stream_reader(raw)
  df <- arrow_as_tibble(arrow_read_record_batch(reader))
  expect_equal(nrow(df), 3)
  expect_equal(df$x, 1:3)
})

test_that("spark_get_java handles invalid and unset JAVA_HOME", {
  withr::local_envvar(JAVA_HOME = "/invalid/java/home")
  expect_error(spark_get_java(throws = TRUE), "does not point to a valid")
  expect_identical(spark_get_java(throws = FALSE), "")

  # JAVA_HOME unset -> falls back to Sys.which("java"); mock Sys.which so this
  # doesn't depend on the host PATH
  withr::local_envvar(JAVA_HOME = NA)
  with_mocked_bindings(
    Sys.which = function(...) c(java = "/usr/bin/java"),
    .package = "base",
    expect_identical(spark_get_java(), c(java = "/usr/bin/java"))
  )
})

test_that("java_is_x64 returns a logical (and FALSE when java is absent)", {
  expect_type(java_is_x64(), "logical")
  with_mocked_bindings(
    spark_get_java = function(...) "",
    .package = "sparklyr",
    expect_false(java_is_x64())
  )
})

test_that("validate_java_version short-circuits for remote masters and guards missing Java", {
  # non-local master with SPARK_HOME set returns TRUE without probing Java
  expect_true(validate_java_version("spark://host:7077", "/some/spark/home"))
  with_mocked_bindings(
    spark_get_java = function(...) "",
    .package = "sparklyr",
    expect_error(validate_java_version("local", ""), "Java is required")
  )
})

test_that("validate_java_version_line errors on unparseable input", {
  expect_error(
    validate_java_version_line("local", character(0)),
    "not detected"
  )
  expect_error(
    validate_java_version_line("local", "garbage with no ver"),
    "couldn't parse"
  )
})

test_clear_cache()
