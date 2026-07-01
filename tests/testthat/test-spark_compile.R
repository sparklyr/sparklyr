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
  # list_sparklyr_jars() looks in <root>/inst/java, which only exists in a
  # source checkout -- on an installed package inst/java is relocated to
  # <pkg>/java. This (build-time) function is only ever called from source, so
  # skip when the source layout isn't present (installed-package test run).
  skip_if(
    !dir.exists(file.path(rprojroot::find_package_root_file(), "inst", "java")),
    "package source tree not available (installed-package test run)"
  )
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

# ---- End-to-end build -> update -> verify (real toolchain) ------------------
# One real build covers the shell-out core of three functions instead of mocking
# them away (which would assert nothing about whether they work):
#   * spark_compile()                  -- real scalac + jar, ONE spec only
#   * spark_update_embedded_sources()  -- real `jar uf` to refresh embedded srcs
#   * spark_verify_embedded_sources()  -- real `jar xf` + diffobj, BOTH branches
# We only fake *scope* -- sparklyr_jar_spec_list() (build one spec, not four) and
# list_sparklyr_jars() (point at the throwaway jar, never inst/java) -- never the
# work. CI installs Scala via download_scalac() and a Spark version for the
# suite, so the toolchain is present; locally it skips unless a matching scalac
# is installed. The repo is left byte-identical (jar in a tempdir, working dir
# and java/embedded_sources.R restored). It can't assert the jar loads into a
# JVM -- that's covered downstream by the suite running against the built jars.
# No skip_on_cran(): the find_scalac() guard already skips when the toolchain is
# absent (as on CRAN), and skip_on_cran() would also hide this from local covr.

test_that("compile, then update + verify embedded sources (real build)", {
  skip_on_livy()
  skip_on_arrow_devel()

  installed <- spark_installed_versions()
  skip_if(nrow(installed) == 0, "no Spark installed")

  # Prefer a 3.x install (Scala 2.12, the canonical build target); otherwise
  # take the first install and pair Scala by Spark major version.
  prefer <- grep("^3[.]", installed$spark)
  sv <- installed$spark[[if (length(prefer)) prefer[[1]] else 1L]]
  major <- as.integer(strsplit(sv, "[.]")[[1]][[1]])
  scala_v <- switch(as.character(major), "2" = "2.11", "3" = "2.12", "2.13")

  scalac <- tryCatch(find_scalac(scala_v), error = function(e) NULL)
  skip_if(is.null(scalac), sprintf("scala %s compiler not installed", scala_v))

  # This test compiles from and rewrites the package *source* tree
  # (java/*.scala, R/worker*.R, java/embedded_sources.R). Those files are not
  # present when the suite runs against an *installed* package (covr::codecov,
  # R CMD check), so skip there. It still runs from a source checkout -- e.g.
  # local `coverage_lines()`, which measures from the repo root even though it
  # sets CODE_COVERAGE=true (so skip_covr() would wrongly skip it there too).
  root <- rprojroot::find_package_root_file()
  embedded_src <- file.path(root, "java", "embedded_sources.R")
  skip_if(
    !file.exists(embedded_src),
    "package source tree not available (installed-package test run)"
  )

  # spark_gen_embedded_sources() reads dir("R"), and update() rewrites
  # java/embedded_sources.R -- run from the package root and restore that file so
  # the repo is left untouched.
  withr::local_dir(root)
  embedded_src <- file.path("java", "embedded_sources.R")
  original_embedded <- readLines(embedded_src)
  withr::defer(writeLines(original_embedded, embedded_src))

  # Build the jar into a tempdir, NOT the package's inst/java.
  withr::local_envvar(R_SPARKINSTALL_COMPILE_JAR_PATH = withr::local_tempdir())

  # 1. Build one jar for real (mock only the spec list, down to one entry).
  with_mocked_bindings(
    sparklyr_jar_spec_list = function() list(list(spark = sv, scala = scala_v)),
    {
      spec <- spark_default_compilation_spec()
      expect_length(spec, 1)
      suppressMessages(compile_package_jars(spec = spec))
    },
    .package = "sparklyr"
  )
  built <- normalizePath(list.files(
    Sys.getenv("R_SPARKINSTALL_COMPILE_JAR_PATH"),
    pattern = "[.]jar$",
    full.names = TRUE
  ))
  expect_length(built, 1)

  # 2. update() refreshes the jar's embedded sources; 3. verify() then finds them
  #    in sync. Mock list_sparklyr_jars() so both touch ONLY the throwaway jar.
  with_mocked_bindings(
    list_sparklyr_jars = function() built,
    {
      suppressMessages(spark_update_embedded_sources())
      expect_no_error(suppressMessages(spark_verify_embedded_sources()))
    },
    .package = "sparklyr"
  )

  # 4. verify()'s failure branch, made deterministic by tampering a copy (rather
  #    than relying on the shipped jars being stale): splice bogus sources in and
  #    confirm verify stops.
  tampered <- file.path(withr::local_tempdir(), basename(built))
  file.copy(built, tampered)
  bad_dir <- withr::local_tempdir()
  dir.create(file.path(bad_dir, "sparklyr"))
  writeLines(
    "# not the real embedded sources",
    file.path(bad_dir, "sparklyr", "embedded_sources.R")
  )
  withr::with_dir(
    bad_dir,
    system2(
      "jar",
      c("uf", tampered, file.path("sparklyr", "embedded_sources.R"))
    )
  )
  with_mocked_bindings(
    list_sparklyr_jars = function() tampered,
    expect_error(suppressMessages(spark_verify_embedded_sources())),
    .package = "sparklyr"
  )
})

test_clear_cache()
