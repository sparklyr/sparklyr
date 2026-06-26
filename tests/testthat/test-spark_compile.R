# Tests for R/spark_compile.R
#
# Ordering:
#   1. The scalac discovery / default-spec tests (formerly test-compile.R).
#      These run in CI (no skip_on_ci()).
#   2. Pure-R unit tests for the helper logic (no Spark, no network).
#   3. The end-to-end jar build test, which is intentionally skipped: it is too
#      slow for routine/CI runs. Retained for manual/local use, not deleted.

test_that("'find_scalac' can find scala version", {
  skip_on_livy()
  skip_on_arrow_devel()
  ensure_download_scalac(scalac_download_path)
  expect_true(scalac_is_available("2.11", scalac_download_path))
  expect_true(scalac_is_available("2.12", scalac_download_path))
})

test_that("'spark_default_compilation_spec' can create default specification", {
  skip_on_livy()
  skip_on_arrow_devel()
  ensure_download_scalac(scalac_download_path)
  dp <- list.files(scalac_download_path)
  expect_gte(length(dp), 3)
})

# ---- Pure-R unit tests (no Spark connection, no network) ---------------------

test_that("sparklyr_jar_spec_list() returns the expected build specs", {
  specs <- sparklyr_jar_spec_list()
  expect_length(specs, 4)
  expect_setequal(
    vapply(specs, function(x) x$spark, character(1)),
    c("2.4.8", "3.0.3", "3.5.4", "4.0.0")
  )
  # the 4.0.0 spec carries an explicit jar_name
  v4 <- Filter(function(x) x$spark == "4.0.0", specs)[[1]]
  expect_identical(v4$jar_name, "sparklyr-master-2.13.jar")
})

test_that("scalac_default_locations() includes the home scala directory", {
  expect_true(path.expand("~/scala") %in% scalac_default_locations())
})

test_that("spark_compilation_spec() echoes supplied version and home", {
  spec <- spark_compilation_spec(
    spark_version = "2.4.8",
    spark_home = "/path/to/spark",
    jar_name = "test.jar"
  )
  expect_identical(spec$spark_version, "2.4.8")
  expect_identical(spec$spark_home, "/path/to/spark")
  expect_identical(spec$jar_name, "test.jar")
  expect_identical(spec$embedded_srcs, "embedded_sources.R")
})

test_that("find_jar() returns NULL when JAVA_HOME is unset", {
  withr::with_envvar(list(JAVA_HOME = ""), {
    expect_null(find_jar())
  })
})

test_that("find_jar() resolves the jar tool under JAVA_HOME", {
  fake_home <- withr::local_tempdir()
  dir.create(file.path(fake_home, "bin"))
  jar_bin <- file.path(fake_home, "bin", "jar")
  file.create(jar_bin)
  withr::with_envvar(list(JAVA_HOME = fake_home), {
    expect_identical(find_jar(), normalizePath(jar_bin, mustWork = FALSE))
  })
})

test_that("find_scalac() errors when no compiler is discovered", {
  empty <- withr::local_tempdir()
  expect_error(
    find_scalac("9.9", locations = empty),
    "failed to discover scala"
  )
})

test_that("make_version_filter() keeps non-versioned and in-range files", {
  filter <- make_version_filter("3.0.0")
  files <- c(
    "java/spark/Common.scala", # no version in dir -> always kept
    "java/spark-2.4.0/Old.scala", # <= 3.0.0 -> kept
    "java/spark-4.0.0/New.scala" # > 3.0.0 -> dropped
  )
  kept <- filter(files)
  expect_true("java/spark/Common.scala" %in% kept)
  expect_true("java/spark-2.4.0/Old.scala" %in% kept)
  expect_false("java/spark-4.0.0/New.scala" %in% kept)
})

test_that("make_version_filter() keeps only the newest of duplicate names", {
  filter <- make_version_filter("4.0.0")
  files <- c(
    "java/spark-2.4.0/Dup.scala",
    "java/spark-3.0.0/Dup.scala",
    "java/spark-4.0.0/Dup.scala"
  )
  expect_identical(filter(files), "java/spark-4.0.0/Dup.scala")
})

test_that("make_version_filter('master') resolves duplicates to the newest", {
  filter <- make_version_filter("master")
  files <- c("java/spark-2.4.0/A.scala", "java/spark-4.0.0/A.scala")
  expect_identical(filter(files), "java/spark-4.0.0/A.scala")
})

# ---- End-to-end jar build (intentionally skipped: too slow for CI) -----------

test_that("jar file is created", {
  skip(
    "This test takes too long to run, and is not necessary for daily operations"
  )
  skip_connection("spark_compile")

  number_of_jars <- 4

  jar_folder <- path.expand("~/testjar")

  s_version <- testthat_spark_env_version()

  major_v <- strsplit(s_version, "\\.")[[1]][[1]]

  if (major_v >= 1) {
    scala_v <- "2.10"
  }
  if (major_v >= 2) {
    scala_v <- "2.11"
  }
  if (major_v >= 3) {
    scala_v <- "2.12"
  }

  jar_name <- sprintf("%s-%s-test.jar", s_version, scala_v)

  scs <- spark_compilation_spec(
    spark_version = s_version,
    scalac_path = find_scalac(scala_v),
    jar_name = jar_name,
    jar_path = find_jar(),
    scala_filter = make_version_filter(s_version)
  )

  Sys.setenv("R_SPARKINSTALL_COMPILE_JAR_PATH" = jar_folder)

  compile_package_jars(scs)

  expect_true(
    file.exists(file.path(jar_folder, jar_name))
  )

  expect_message(
    sparklyr_jar_verify_spark(),
    "- Spark version"
  )

  expect_length(
    sparklyr_jar_spec_list(),
    number_of_jars
  )

  expect_silent(
    download_scalac()
  )

  expect_is(
    make_version_filter(s_version),
    "function"
  )
})

test_clear_cache()
