skip_connection("spark_context")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("configuration getter works", {
  if (spark_version(sc) >= "2.0.0") {
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 1L)
    expect_equal(
      "1",
      unname(unlist(spark_session_config(
        sc,
        "spark.sql.shuffle.partitions.local"
      )))
    )

    # make sure we didn't just get lucky
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 2L)
    expect_equal(
      "2",
      unname(unlist(spark_session_config(
        sc,
        "spark.sql.shuffle.partitions.local"
      )))
    )
  }
})

test_that("spark_session_config() rejects unsupported value types", {
  expect_error(
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", list(1)),
    "logical, integer .long., and character"
  )
})

test_that("spark_adaptive_query_execution() works", {
  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_adaptive_query_execution(sc, TRUE)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_adaptive_query_execution(sc) %>% unname() %>% unlist(),
    "true"
  )
})

test_that("spark_coalesce_shuffle_partitions() works", {
  test_requires_version("3.0.0")

  spark_session_config(
    sc,
    "spark.sql.adaptive.coalescePartitions.enabled",
    FALSE
  )
  spark_coalesce_shuffle_partitions(sc, TRUE)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.enabled"
    ) %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_coalesce_shuffle_partitions(sc) %>% unname() %>% unlist(),
    "true"
  )
})

test_that("spark_advisory_shuffle_partition_size() works", {
  test_requires_version("3.0.0")

  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_session_config(
    sc,
    "spark.sql.adaptive.coalescePartitions.enabled",
    FALSE
  )

  advisory_shuffle_partition_size <- 64 * 1024 * 1024
  spark_advisory_shuffle_partition_size(sc, advisory_shuffle_partition_size)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.enabled"
    ) %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.advisoryPartitionSizeInBytes"
    ) %>%
      unname() %>%
      unlist(),
    as.character(advisory_shuffle_partition_size)
  )
  expect_equal(
    spark_advisory_shuffle_partition_size(sc) %>% unname() %>% unlist(),
    as.character(advisory_shuffle_partition_size)
  )
})

test_that("spark_coalesce_initial_num_partitions() works", {
  test_requires_version("3.0.0")

  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_session_config(
    sc,
    "spark.sql.adaptive.coalescePartitions.enabled",
    FALSE
  )

  num_partitions <- 64
  spark_coalesce_initial_num_partitions(sc, num_partitions)

  expect_equal(
    spark_session_config(sc, "spark.sql.adaptive.enabled") %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.enabled"
    ) %>%
      unname() %>%
      unlist(),
    "true"
  )
  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
    ) %>%
      unname() %>%
      unlist(),
    as.character(num_partitions)
  )
  expect_equal(
    spark_coalesce_initial_num_partitions(sc) %>% unname() %>% unlist(),
    as.character(num_partitions)
  )
})

test_that("spark_auto_broadcast_join_threshold() works", {
  spark_session_config(sc, "spark.sql.autoBroadcastJoinThreshold", -1)
  threshold <- 10 * 1024 * 1024
  spark_auto_broadcast_join_threshold(sc, threshold)

  expect_equal(
    spark_session_config(sc, "spark.sql.autoBroadcastJoinThreshold") %>%
      unname() %>%
      unlist(),
    as.character(threshold)
  )
})

test_that("spark_coalesce_min_num_partitions() works", {
  test_requires_version("3.0.0")

  spark_session_config(sc, "spark.sql.adaptive.enabled", FALSE)
  spark_session_config(
    sc,
    "spark.sql.adaptive.coalescePartitions.enabled",
    FALSE
  )

  num_partitions <- 4
  spark_coalesce_min_num_partitions(sc, num_partitions)

  expect_equal(
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.minPartitionNum"
    ) %>%
      unname() %>%
      unlist(),
    as.character(num_partitions)
  )
  expect_equal(
    spark_coalesce_min_num_partitions(sc) %>% unname() %>% unlist(),
    as.character(num_partitions)
  )
})

test_that("spark_context_config() and hive_context_config() return named entries", {
  cfg <- spark_context_config(sc)
  expect_type(cfg, "list")
  expect_true(length(names(cfg)) > 0)

  hive_cfg <- hive_context_config(sc)
  expect_type(hive_cfg, "list")
})

test_that("spark_version() returns a cleaned, cached numeric_version", {
  v <- spark_version(sc)
  expect_s3_class(v, "numeric_version")
  # second call should hit the cached branch and return the same value
  expect_identical(spark_version(sc), v)
})

# ---- Connection-free helpers ----------------------------------------------

test_that("spark_version_clean() strips suffixes and trailing dots", {
  expect_equal(spark_version_clean("2.4.8"), "2.4.8")
  expect_equal(spark_version_clean("3.0.0-preview"), "3.0.0")
  expect_equal(spark_version_clean("3.5.0-SNAPSHOT"), "3.5.0")
})

test_that("spark_version_from_home_version() reads the env variable", {
  withr::with_envvar(c(SPARK_HOME_VERSION = "3.2.1"), {
    expect_equal(spark_version_from_home_version(), "3.2.1")
  })
  withr::with_envvar(c(SPARK_HOME_VERSION = ""), {
    expect_null(spark_version_from_home_version())
  })
})

test_that("spark_version_from_home() resolves through each detection strategy", {
  empty_home <- withr::local_tempdir()

  # useDefault: a supplied default short-circuits everything
  expect_equal(
    spark_version_from_home(empty_home, default = "3.1.2"),
    "3.1.2"
  )

  withr::with_envvar(c(SPARK_HOME_VERSION = ""), {
    # useEnvironmentVariable
    withr::with_envvar(c(SPARK_HOME_VERSION = "3.2.1"), {
      expect_equal(spark_version_from_home(empty_home), "3.2.1")
    })

    # useReleaseFile
    release_home <- withr::local_tempdir()
    writeLines(
      "Spark 3.3.0 built for Hadoop 3.3.4",
      file.path(release_home, "RELEASE")
    )
    expect_equal(spark_version_from_home(release_home), "3.3.0")

    # useAssemblies
    asm_home <- withr::local_tempdir()
    dir.create(file.path(asm_home, "lib"))
    file.create(file.path(
      asm_home,
      "lib",
      "spark-assembly-2.4.0-hadoop2.7.jar"
    ))
    expect_equal(spark_version_from_home(asm_home), "2.4.0")

    # useSparkSubmit: reached only when the file-based strategies find nothing
    with_mocked_bindings(
      system2 = function(...) c("Welcome", "   version 3.4.1", "Using"),
      .package = "base",
      expect_equal(spark_version_from_home(empty_home), "3.4.1")
    )

    # all strategies exhausted -> error
    with_mocked_bindings(
      system2 = function(...) c("no version here"),
      .package = "base",
      expect_error(
        spark_version_from_home(empty_home),
        "Failed to detect version"
      )
    )
  })
})

test_that("spark_version_latest() picks the newest matching version", {
  with_mocked_bindings(
    spark_available_versions = function(...) {
      data.frame(spark = c("3.4.1", "4.0.0", "3.5.0"))
    },
    .package = "sparklyr",
    {
      expect_equal(spark_version_latest(), "4.0.0")
      expect_equal(spark_version_latest("3.5"), "3.5.0")
      # no match falls back to the overall latest
      expect_equal(spark_version_latest("2.1"), "4.0.0")
    }
  )
})

test_that("spark_home_dir() returns the install dir or NULL on failure", {
  with_mocked_bindings(
    spark_install_find = function(...) list(sparkVersionDir = "/opt/spark"),
    .package = "sparklyr",
    expect_equal(spark_home_dir(version = "3.5.0"), "/opt/spark")
  )

  with_mocked_bindings(
    spark_install_find = function(...) stop("not found"),
    .package = "sparklyr",
    expect_null(spark_home_dir(version = "9.9.9"))
  )
})

test_that("spark_home_set() sets SPARK_HOME, with and without a path", {
  withr::local_envvar(c(SPARK_HOME = ""))

  spark_home_set(file.path("custom", "spark"))
  expect_equal(Sys.getenv("SPARK_HOME"), file.path("custom", "spark"))

  # NULL path -> looks up an install and emits the verbose message
  with_mocked_bindings(
    spark_install_find = function(...) list(sparkVersionDir = "/opt/latest"),
    .package = "sparklyr",
    expect_message(spark_home_set(), "Setting SPARK_HOME")
  )
  expect_equal(Sys.getenv("SPARK_HOME"), "/opt/latest")
})

# ---- Session-level variable accessors (genv_*) ----------------------------

test_that("genv_* getters and setters round-trip session state", {
  restore <- function(getter, setter) {
    orig <- getter()
    withr::defer(setter(orig), envir = parent.frame())
  }

  restore(genv_get_last_error, genv_set_last_error)
  genv_set_last_error("boom")
  expect_equal(genv_get_last_error(), "boom")

  restore(genv_get_extension_packages, genv_set_extension_packages)
  genv_set_extension_packages(c("pkgA", "pkgB"))
  expect_equal(genv_get_extension_packages(), c("pkgA", "pkgB"))

  restore(genv_get_spark_versions_json, genv_set_spark_versions_json)
  genv_set_spark_versions_json(list(a = 1))
  expect_equal(genv_get_spark_versions_json(), list(a = 1))

  restore(genv_get_param_mapping_s_to_r, genv_set_param_mapping_s_to_r)
  genv_set_param_mapping_s_to_r(list(s = "r"))
  expect_equal(genv_get_param_mapping_s_to_r(), list(s = "r"))

  restore(genv_get_param_mapping_r_to_s, genv_set_param_mapping_r_to_s)
  genv_set_param_mapping_r_to_s(list(r = "s"))
  expect_equal(genv_get_param_mapping_r_to_s(), list(r = "s"))

  restore(genv_get_ml_class_mapping, genv_set_ml_class_mapping)
  genv_set_ml_class_mapping(list(cls = "ml"))
  expect_equal(genv_get_ml_class_mapping(), list(cls = "ml"))

  restore(genv_get_ml_package_mapping, genv_set_ml_package_mapping)
  genv_set_ml_package_mapping(list(cls = "pkg"))
  expect_equal(genv_get_ml_package_mapping(), list(cls = "pkg"))

  restore(genv_get_avail_package_cache, genv_set_avail_package_cache)
  genv_set_avail_package_cache(list(cache = TRUE))
  expect_equal(genv_get_avail_package_cache(), list(cache = TRUE))
})

test_that("genv do_spark options helpers set, read, and clear", {
  orig <- genv_get_do_spark()
  withr::defer(genv_set_do_spark(orig))

  genv_set_do_spark(list(name = "session", options = new.env()))
  expect_equal(genv_get_do_spark("name"), "session")
  expect_type(genv_get_do_spark(), "list")

  genv_set_do_spark_options(c("opt1", "opt2"), list(10L, 20L))
  expect_equal(get("opt1", envir = .gls_env$do_spark$options), 10L)
  expect_equal(get("opt2", envir = .gls_env$do_spark$options), 20L)

  genv_clear_do_spark_options()
  expect_length(ls(.gls_env$do_spark$options), 0)
})

test_clear_cache()
