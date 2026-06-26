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

test_that("find_scalac() resolves a discovered compiler and skips dirs without one", {
  loc <- withr::local_tempdir()
  # a matching install that lacks the bin/scalac binary -> skipped -> errors
  dir.create(file.path(loc, "scala-2.12.0"))
  expect_error(find_scalac("2.12", locations = loc), "failed to discover scala")

  # a matching install with the binary present -> returned
  dir.create(file.path(loc, "scala-2.12.5", "bin"), recursive = TRUE)
  scalac <- file.path(loc, "scala-2.12.5", "bin", "scalac")
  file.create(scalac)
  expect_identical(find_scalac("2.12", locations = loc), scalac)
})

test_that("scalac_default_locations() returns a single path on Windows", {
  with_mocked_bindings(
    os_is_windows = function() TRUE,
    .package = "sparklyr",
    expect_identical(scalac_default_locations(), path.expand("~/scala"))
  )
})

test_that("get_scalac_version() parses the version out of scalac -version", {
  with_mocked_bindings(
    system = function(...) "Scala compiler version 2.12.20 -- Copyright",
    .package = "base",
    expect_identical(get_scalac_version("scalac"), "2.12.20")
  )
})

test_that("find_jar() falls back to `which jar` when not under JAVA_HOME", {
  fake_home <- withr::local_tempdir() # no bin/jar inside
  with_mocked_bindings(
    system2 = function(...) "/usr/bin/jar",
    .package = "base",
    withr::with_envvar(list(JAVA_HOME = fake_home), {
      expect_identical(find_jar(), "/usr/bin/jar")
    })
  )
})

test_that("list_sparklyr_jars() finds the shipped sparklyr jars", {
  jars <- list_sparklyr_jars()
  expect_true(length(jars) > 0)
  expect_true(all(grepl("sparklyr-.+\\.jar$", basename(jars))))
})

test_that("spark_gen_embedded_sources() writes the worker bootstrap line", {
  out <- withr::local_tempfile(fileext = ".R")
  spark_gen_embedded_sources(output = out)
  lines <- readLines(out)
  expect_match(
    lines[[length(lines)]],
    "do.call\\(spark_worker_main"
  )
})

test_that("sparklyr_jar_verify_spark() reports installed and missing versions", {
  # all specs already installed -> "Ok"
  with_mocked_bindings(
    spark_installed_versions = function() {
      data.frame(spark = c("2.4.8", "3.0.3", "3.5.4", "4.0.0"))
    },
    .package = "sparklyr",
    expect_message(sparklyr_jar_verify_spark(), "Ok")
  )

  # nothing installed, install = FALSE -> "Not found" without installing
  with_mocked_bindings(
    spark_installed_versions = function() data.frame(spark = character()),
    .package = "sparklyr",
    expect_message(sparklyr_jar_verify_spark(install = FALSE), "Not found")
  )

  # nothing installed, install = TRUE -> spark_install is invoked
  installed <- character()
  with_mocked_bindings(
    spark_installed_versions = function() data.frame(spark = character()),
    spark_install = function(version, ...) installed <<- c(installed, version),
    .package = "sparklyr",
    suppressMessages(sparklyr_jar_verify_spark(install = TRUE))
  )
  expect_setequal(installed, c("2.4.8", "3.0.3", "3.5.4", "4.0.0"))
})

test_that("spark_default_compilation_spec() builds one spec per jar target", {
  with_mocked_bindings(
    find_scalac = function(version, locations) "/path/to/scalac",
    find_jar = function() "/path/to/jar",
    spark_home_dir = function(version, ...) "/path/to/home",
    .package = "sparklyr",
    {
      specs <- spark_default_compilation_spec(pkg = "sparklyr")
      expect_length(specs, 4)
      expect_setequal(
        vapply(specs, function(x) x$spark_version, character(1)),
        c("2.4.8", "3.0.3", "3.5.4", "4.0.0")
      )
      # the 4.0.0 spec carries the explicit jar name from the spec list
      v4 <- Filter(function(x) x$spark_version == "4.0.0", specs)[[1]]
      expect_identical(v4$jar_name, "sparklyr-master-2.13.jar")
    }
  )
})

test_that("compile_package_jars() handles explicit, master, and download specs", {
  compiled <- list()
  capture <- function(jar_name, spark_home, ...) {
    compiled[[length(compiled) + 1]] <<- list(
      jar_name = jar_name,
      spark_home = spark_home
    )
    TRUE
  }

  # explicit spark_home -> straight to spark_compile, no install
  with_mocked_bindings(
    spark_compile = capture,
    .package = "sparklyr",
    compile_package_jars(
      spec = list(spark_home = "/explicit", jar_name = "explicit.jar")
    )
  )
  expect_identical(compiled[[1]]$spark_home, "/explicit")

  # spark_version == "master" -> resolves home from spark_install_find
  compiled <- list()
  with_mocked_bindings(
    spark_compile = capture,
    spark_install_find = function(...) {
      list(sparkVersion = "3.5.4", sparkVersionDir = "/installed/home")
    },
    .package = "sparklyr",
    compile_package_jars(
      spec = list(spark_version = "master", jar_name = "master.jar")
    )
  )
  expect_identical(compiled[[1]]$spark_home, "/installed/home")

  # spark_version set -> downloads + resolves home via spark_home_dir
  compiled <- list()
  with_mocked_bindings(
    spark_compile = capture,
    spark_install = function(...) invisible(NULL),
    spark_home_dir = function(version, ...) "/downloaded/home",
    .package = "sparklyr",
    suppressMessages(compile_package_jars(
      spec = list(spark_version = "3.5.4", jar_name = "dl.jar")
    ))
  )
  expect_identical(compiled[[1]]$spark_home, "/downloaded/home")

  # no args -> falls back to spark_default_compilation_spec
  compiled <- list()
  with_mocked_bindings(
    spark_compile = capture,
    spark_default_compilation_spec = function(...) {
      list(list(spark_home = "/default", jar_name = "default.jar"))
    },
    .package = "sparklyr",
    compile_package_jars()
  )
  expect_identical(compiled[[1]]$jar_name, "default.jar")
})

test_that("download_scalac() downloads + extracts, and skips existing files", {
  # tgz path (non-Windows): downloads and untars each compiler
  dest <- withr::local_tempdir()
  downloaded <- character()
  with_mocked_bindings(
    download_file = function(url, destfile, ...) {
      downloaded <<- c(downloaded, basename(destfile))
      file.create(destfile)
      0L
    },
    untar = function(...) 0L,
    .package = "sparklyr",
    suppressMessages(download_scalac(dest_path = dest))
  )
  expect_length(downloaded, 3)

  # second run with files already present -> nothing re-downloaded
  downloaded <- character()
  with_mocked_bindings(
    download_file = function(url, destfile, ...) {
      downloaded <<- c(downloaded, basename(destfile))
      0L
    },
    untar = function(...) 0L,
    .package = "sparklyr",
    suppressMessages(download_scalac(dest_path = dest))
  )
  expect_length(downloaded, 0)

  # zip path (Windows): unzip instead of untar
  dest_win <- withr::local_tempdir()
  unzipped <- 0
  with_mocked_bindings(
    os_is_windows = function() TRUE,
    download_file = function(url, destfile, ...) {
      file.create(destfile)
      0L
    },
    unzip = function(...) unzipped <<- unzipped + 1,
    .package = "sparklyr",
    suppressMessages(download_scalac(dest_path = dest_win))
  )
  expect_equal(unzipped, 3)
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
