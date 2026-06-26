# Returns a download_file() mock that drops a minimal fake Spark tarball at
# `destfile`, so spark_install() can run end-to-end (untar + config writers)
# without a real network download. `log4j` selects which template ships:
# "v2" (log4j2.properties.template), "v1" (log4j.properties.template), or
# "none" (no template, to exercise the missing-template error path).
make_fake_download <- function(log4j = "v2") {
  function(url, destfile, ...) {
    component <- sub("\\.tgz$", "", basename(destfile))
    staging <- tempfile("sparkfix")
    conf_dir <- file.path(staging, component, "conf")
    dir.create(conf_dir, recursive = TRUE)
    if (log4j == "v2") {
      # "rootLogger.level=" (no spaces) so the property-replace (gsub) branch
      # fires for it, while the other properties exercise the append branch.
      writeLines(
        c("rootLogger.level=info", "logger.jetty.level = warn"),
        file.path(conf_dir, "log4j2.properties.template")
      )
    } else if (log4j == "v1") {
      writeLines(
        c("log4j.rootCategory=INFO, console", "log4j.appender.localfile=x"),
        file.path(conf_dir, "log4j.properties.template")
      )
    }
    # A pre-existing "spark.local.dir" line triggers the gsub branch in the
    # spark-defaults writer (only reached when properties are non-empty, i.e.
    # on Windows); other properties hit the append branch.
    writeLines(
      c(
        "# Default system properties included when running spark-submit.",
        "spark.local.dir              /tmp/old"
      ),
      file.path(conf_dir, "spark-defaults.conf.template")
    )
    withr::with_dir(
      staging,
      utils::tar(
        destfile,
        files = component,
        compression = "gzip",
        tar = "internal"
      )
    )
    0L
  }
}

fake_spark_download <- make_fake_download("v2")

test_that("spark_install downloads, untars, and configures (mocked download)", {
  skip_on_windows()
  test_version <- "3.4.4"
  test_hadoop <- "3"
  component <- sprintf("spark-%s-bin-hadoop%s", test_version, test_hadoop)

  withr::with_options(
    list("spark.install.dir" = tempfile("sparkinstall")),
    {
      install_dir <- spark_install_dir()

      with_mocked_bindings(
        download_file = fake_spark_download,
        .package = "sparklyr",
        {
          info <- spark_install(
            version = test_version,
            hadoop_version = test_hadoop,
            verbose = FALSE
          )
        }
      )

      conf <- file.path(install_dir, component, "conf")
      expect_true(dir.exists(file.path(install_dir, component)))
      expect_true(file.exists(file.path(conf, "log4j2.properties")))
      expect_true(file.exists(file.path(conf, "spark-defaults.conf")))
      expect_true(file.exists(file.path(conf, "hive-site.xml")))
      expect_equal(info$sparkVersion, test_version)
    }
  )
})

test_that("spark_default_version uses bundled default when nothing installed", {
  with_mocked_bindings(
    spark_installed_versions = function() data.frame(),
    .package = "sparklyr",
    {
      default <- spark_default_version()
    }
  )

  expect_equal(names(default), c("spark", "hadoop"))
  expect_equal(default$spark, "2.4.3")
  expect_equal(default$hadoop, "2.7")
})

test_that("spark_default_version uses installed version when one exists", {
  with_mocked_bindings(
    spark_installed_versions = function() {
      data.frame(spark = "3.4.4", hadoop = "3", stringsAsFactors = FALSE)
    },
    spark_install_find = function(...) {
      list(sparkVersion = "3.4.4", hadoopVersion = "3")
    },
    .package = "sparklyr",
    {
      default <- spark_default_version()
    }
  )

  expect_equal(default$spark, "3.4.4")
  expect_equal(default$hadoop, "3")
})

test_that("spark_resolve_envpath expands env vars on Windows", {
  with_mocked_bindings(
    os_is_windows = function() TRUE,
    .package = "sparklyr",
    {
      # env var set -> first segment substituted
      withr::with_envvar(
        c("SPARKLYR_TEST_HOME" = file.path("C:", "Users", "test")),
        resolved <- spark_resolve_envpath("%SPARKLYR_TEST_HOME%/spark")
      )
      expect_equal(resolved, file.path("C:", "Users", "test", "spark"))

      # env var not set -> %VAR% kept as a literal segment
      withr::with_envvar(
        c("SPARKLYR_NOT_SET" = NA),
        unresolved <- spark_resolve_envpath("%SPARKLYR_NOT_SET%/spark")
      )
      expect_equal(unresolved, file.path("%SPARKLYR_NOT_SET%", "spark"))
    }
  )
})

test_that("spark_can_install checks the install dir", {
  # non-existent dir -> installable
  withr::with_options(
    list("spark.install.dir" = tempfile("noexist")),
    expect_true(spark_can_install())
  )
  # existing writable dir -> installable
  writable <- tempfile("installable")
  dir.create(writable)
  withr::with_options(
    list("spark.install.dir" = writable),
    expect_true(spark_can_install())
  )
})

test_that("spark_home reflects the SPARK_HOME env var", {
  withr::with_envvar(
    c("SPARK_HOME" = file.path("opt", "spark")),
    expect_equal(spark_home(), file.path("opt", "spark"))
  )
  withr::with_envvar(
    c("SPARK_HOME" = NA),
    expect_null(spark_home())
  )
})

test_that("spark_install_version_expand resolves installed-only lookups", {
  with_mocked_bindings(
    spark_installed_versions = function() {
      data.frame(spark = c("3.4.4", "3.5.1"), stringsAsFactors = FALSE)
    },
    .package = "sparklyr",
    expect_equal(spark_install_version_expand("3.4", TRUE), "3.4.4")
  )
})

test_that("spark_install_find warns and builds a fallback for unknown versions", {
  expect_warning(
    info <- spark_install_find(version = "1.1", hadoop_version = "2.7"),
    "version specified may not be available"
  )
  expect_equal(info$componentName, "spark-1.1-bin-hadoop2.7")
  expect_equal(info$packageName, "spark-1.1-bin-hadoop2.7.tgz")
  expect_match(info$packageRemotePath, "archive.apache.org")

  # the "master" path filters to installed versions; with an empty install dir
  # nothing is installed, so it funnels through the fallback (and warns)
  withr::with_options(
    list("spark.install.dir" = tempfile("nomaster")),
    expect_warning(spark_install_find(version = "master"))
  )
})

test_that("spark_install_tar validates and extracts a tarball", {
  expect_error(
    spark_install_tar(tempfile("missing", fileext = ".tgz")),
    "does not exist"
  )

  bad <- tempfile(fileext = ".tgz")
  file.create(bad)
  expect_error(spark_install_tar(bad), "does not conform")

  component <- "spark-3.4.4-bin-hadoop3"
  staging <- tempfile("tarfix")
  dir.create(file.path(staging, component, "conf"), recursive = TRUE)
  writeLines("x", file.path(staging, component, "conf", "marker"))
  tarpath <- file.path(staging, paste0(component, ".tgz"))
  withr::with_dir(
    staging,
    utils::tar(
      tarpath,
      files = component,
      compression = "gzip",
      tar = "internal"
    )
  )

  withr::with_options(
    list("spark.install.dir" = tempfile("tarinstall")),
    {
      install_dir <- spark_install_dir()
      dir.create(install_dir, recursive = TRUE)
      spark_install_tar(tarpath)
      expect_true(dir.exists(file.path(install_dir, component)))
    }
  )
})

test_that("spark_uninstall removes an install and reports when absent", {
  skip_on_windows()
  test_version <- "3.4.4"
  test_hadoop <- "3"
  component <- sprintf("spark-%s-bin-hadoop%s", test_version, test_hadoop)

  withr::with_options(
    list("spark.install.dir" = tempfile("uninstall")),
    {
      install_dir <- spark_install_dir()

      expect_message(
        absent <- spark_uninstall(test_version, test_hadoop),
        "not found"
      )
      expect_false(absent)

      with_mocked_bindings(
        download_file = fake_spark_download,
        .package = "sparklyr",
        spark_install(test_version, test_hadoop, verbose = FALSE)
      )

      expect_message(
        removed <- spark_uninstall(test_version, test_hadoop),
        "successfully uninstalled"
      )
      expect_true(removed)
      expect_false(dir.exists(file.path(install_dir, component)))
    }
  )
})

test_that("spark_install is verbose and detects an existing install", {
  skip_on_windows()
  withr::with_options(
    list("spark.install.dir" = tempfile("verbose")),
    {
      with_mocked_bindings(
        download_file = fake_spark_download,
        .package = "sparklyr",
        {
          expect_message(
            spark_install("3.4.4", "3", verbose = TRUE),
            "Installing Spark"
          )
          expect_message(
            spark_install("3.4.4", "3", verbose = TRUE),
            "already installed"
          )
        }
      )
    }
  )
})

test_that("spark_install applies Windows-specific configuration", {
  test_version <- "3.4.4"
  test_hadoop <- "3"
  component <- sprintf("spark-%s-bin-hadoop%s", test_version, test_hadoop)

  withr::with_options(
    list("spark.install.dir" = tempfile("wininstall")),
    {
      install_dir <- spark_install_dir()
      with_mocked_bindings(
        os_is_windows = function() TRUE,
        download_file = fake_spark_download,
        .package = "sparklyr",
        spark_install(test_version, test_hadoop, verbose = FALSE)
      )

      conf <- file.path(install_dir, component, "conf")
      defaults <- readLines(file.path(conf, "spark-defaults.conf"))
      expect_true(any(grepl("spark.local.dir", defaults)))
      expect_true(any(grepl("spark.sql.warehouse.dir", defaults)))
    }
  )
})

test_that("spark_install handles the log4j v1 template on Windows", {
  withr::with_options(
    list("spark.install.dir" = tempfile("v1install")),
    {
      install_dir <- spark_install_dir()
      with_mocked_bindings(
        os_is_windows = function() TRUE,
        download_file = make_fake_download("v1"),
        .package = "sparklyr",
        spark_install("3.4.4", "3", verbose = FALSE)
      )
      conf <- file.path(install_dir, "spark-3.4.4-bin-hadoop3", "conf")
      expect_true(file.exists(file.path(conf, "log4j.properties")))
    }
  )
})

test_that("spark_install retries from the archive when the primary fails", {
  skip_on_windows()
  component <- "spark-3.4.4-bin-hadoop3"
  withr::with_options(
    list("spark.install.dir" = tempfile("retry")),
    {
      install_dir <- spark_install_dir()
      fake_info <- list(
        sparkDir = install_dir,
        packageLocalPath = file.path(install_dir, paste0(component, ".tgz")),
        packageRemotePath = paste0(
          "https://dlcdn.apache.org/spark/spark-3.4.4/",
          component,
          ".tgz"
        ),
        sparkVersionDir = file.path(install_dir, component),
        sparkConfDir = file.path(install_dir, component, "conf"),
        sparkVersion = "3.4.4",
        hadoopVersion = "3",
        installed = FALSE
      )
      attempts <- 0
      retry_download <- function(url, destfile, ...) {
        attempts <<- attempts + 1
        if (grepl("dlcdn", url, fixed = TRUE)) {
          return(1L) # primary mirror fails
        }
        fake_spark_download(url, destfile, ...) # archive succeeds
      }

      with_mocked_bindings(
        spark_install_find = function(...) fake_info,
        download_file = retry_download,
        .package = "sparklyr",
        expect_message(
          spark_install("3.4.4", "3", verbose = TRUE),
          "retrying from archive"
        )
      )
      expect_equal(attempts, 2)
      expect_true(dir.exists(file.path(install_dir, component)))
    }
  )
})

test_that("spark_install errors when the download cannot complete", {
  component <- "spark-3.4.4-bin-hadoop3"
  make_info <- function(install_dir) {
    list(
      sparkDir = install_dir,
      packageLocalPath = file.path(install_dir, paste0(component, ".tgz")),
      packageRemotePath = paste0(
        "https://dlcdn.apache.org/spark/spark-3.4.4/",
        component,
        ".tgz"
      ),
      sparkVersionDir = file.path(install_dir, component),
      sparkConfDir = file.path(install_dir, component, "conf"),
      sparkVersion = "3.4.4",
      hadoopVersion = "3",
      installed = FALSE
    )
  }

  withr::with_options(
    list("spark.install.dir" = tempfile("dlerror")),
    {
      fake_info <- make_info(spark_install_dir())
      # download_file throws -> stop(download_result$message)
      with_mocked_bindings(
        spark_install_find = function(...) fake_info,
        download_file = function(...) stop("network is down"),
        .package = "sparklyr",
        expect_error(spark_install("3.4.4", "3"), "network is down")
      )
    }
  )

  withr::with_options(
    list("spark.install.dir" = tempfile("dlstatus")),
    {
      fake_info <- make_info(spark_install_dir())
      # download_file returns non-zero everywhere -> stopf() status message
      with_mocked_bindings(
        spark_install_find = function(...) fake_info,
        download_file = function(...) 19L,
        .package = "sparklyr",
        expect_error(spark_install("3.4.4", "3"), "exited with status 19")
      )
    }
  )
})

test_that("spark_install warns when configuration steps fail", {
  skip_on_windows()
  withr::with_options(
    list("spark.install.dir" = tempfile("confwarn")),
    {
      warns <- with_mocked_bindings(
        download_file = fake_spark_download,
        spark_conf_log4j_set_value = function(...) stop("log4j"),
        spark_hive_file_set_value = function(...) stop("hive"),
        spark_conf_file_set_value = function(...) stop("defaults"),
        .package = "sparklyr",
        capture_warnings(spark_install("3.4.4", "3", verbose = FALSE))
      )
      expect_match(warns, "Failed to set logging settings", all = FALSE)
      expect_match(warns, "Failed to apply custom hive", all = FALSE)
      expect_match(warns, "Failed to set spark-defaults", all = FALSE)
    }
  )
})

test_that("spark_conf_log4j_set_value errors without a template and honors custom properties", {
  conf <- tempfile("conf")
  dir.create(conf)
  info <- list(sparkConfDir = conf)

  # no template present -> error
  expect_error(
    spark_conf_log4j_set_value(info),
    "No log4j template file found"
  )

  # template present + explicit properties -> properties take precedence
  writeLines(
    "rootLogger.level=info",
    file.path(conf, "log4j2.properties.template")
  )
  spark_conf_log4j_set_value(
    info,
    properties = list("custom.prop" = "custom.prop = value")
  )
  written <- readLines(file.path(conf, "log4j2.properties"))
  expect_true(any(grepl("custom.prop = value", written)))
})

test_clear_cache()
