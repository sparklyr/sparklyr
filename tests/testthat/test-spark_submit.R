skip_connection("spark_submit")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("spark_submit() can submit batch jobs", {
  skip_if_dbplyr_dev()
  skip_databricks_connect()
  if (.Platform$OS.type == "windows") {
    skip("spark_submit() not yet implemented for windows")
  }

  batch_file <- dir(
    getwd(),
    recursive = TRUE,
    pattern = "batch.R",
    full.names = TRUE
  )

  if (dir.exists("batch.csv")) {
    unlink("batch.csv", recursive = TRUE)
  }

  sc <- testthat_spark_connection()

  withr::with_options(
    list(
      sparklyr.log.console = FALSE,
      sparklyr.verbose = FALSE
    ),
    {
      spark_submit(
        master = "local",
        file = batch_file,
        version = spark_version(sc)
      )
    }
  )

  retries <- 60
  while (!dir.exists("batch.csv") && retries > 0) {
    Sys.sleep(1)
    retries <- retries - 1
  }

  expect_gt(retries, 0)
})

test_that("spark_dependency_fallback() works correctly", {
  expect_equal(
    spark_dependency_fallback("2.3", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.3")),
    "2.1"
  )
})

test_that("sparklyr.nested can query nested columns", {
  if (!"sparklyr.nested" %in% installed.packages()) {
    skip("sparklyr.nested not installed.")
  }

  iris_tbl <- testthat_tbl("iris")
  iris_nst <- iris_tbl %>% sparklyr.nested::sdf_nest(Species, Sepal_Width)

  expect_equal(
    iris_nst %>%
      filter(data[["Species"]] == "setosa") %>%
      count() %>%
      pull(n) %>%
      as.integer(),
    50
  )
})

test_that("spark_submit() defaults verbose + console logging for batch jobs", {
  # with neither option set, spark_submit turns both on before connecting;
  # shell_connection is mocked so no real batch job is submitted
  withr::local_options(sparklyr.verbose = NULL, sparklyr.log.console = NULL)
  src <- withr::local_tempfile(fileext = ".R")
  writeLines("1", src)

  captured <- NULL
  with_mocked_bindings(
    shell_connection = function(..., config, batch) {
      captured <<- config
      NULL
    },
    .package = "sparklyr",
    spark_submit(master = "local", file = src, config = list())
  )
  expect_true(captured$sparklyr.verbose)
  expect_true(captured$sparklyr.log.console)
})

# ---- Extension registration + dependency resolution (connection-free) ------

test_that("spark_dependency() constructs a dependency object", {
  dep <- spark_dependency(
    jars = "a.jar",
    packages = "g:a:1",
    repositories = "r"
  )
  expect_s3_class(dep, "spark_dependency")
  expect_equal(dep$jars, "a.jar")
  expect_equal(dep$packages, "g:a:1")
  expect_equal(dep$repositories, "r")
  expect_null(dep$initializer)
})

test_that("register_extension appends to registered_extensions", {
  orig <- registered_extensions()
  withr::defer(genv_set_extension_packages(orig))

  genv_set_extension_packages(character())
  register_extension("pkgA")
  register_extension(c("pkgB", "pkgC"))
  expect_equal(registered_extensions(), c("pkgA", "pkgB", "pkgC"))
})

test_that("spark_dependencies() returns sparklyr's own jar dependency", {
  dep <- spark_dependencies(numeric_version("3.5.0"), "2.12")
  expect_s3_class(dep, "spark_dependency")
  expect_match(dep$jars, "sparklyr-3.5-2.12.jar")
})

test_that("spark_dependencies_from_extension finds and calls the extension", {
  # sparklyr itself exports a spark_dependencies() function
  dep <- spark_dependencies_from_extension(
    numeric_version("3.5.0"),
    numeric_version("2.12"),
    "sparklyr"
  )
  expect_type(dep, "list")
  expect_s3_class(dep[[1]], "spark_dependency")
})

test_that("spark_dependencies_from_extension errors when the function is missing", {
  expect_error(
    spark_dependencies_from_extension(
      numeric_version("3.5.0"),
      numeric_version("2.12"),
      "stats"
    ),
    "spark_dependencies function not found"
  )
})

test_that("spark_dependencies_from_extensions derives scala version by spark version", {
  # the scala-version derivation runs before the (empty) extension loop, so
  # these three calls cover the <2.0 / <3.0 / else branches
  for (v in c("1.6.0", "2.4.0", "3.5.0")) {
    res <- spark_dependencies_from_extensions(
      numeric_version(v),
      scala_version = NULL,
      extensions = list(),
      config = list()
    )
    expect_named(
      res,
      c(
        "jars",
        "packages",
        "initializers",
        "catalog_jars",
        "repositories",
        "dbplyr_sql_variant"
      )
    )
  }
})

test_that("spark_dependencies_from_extensions merges jars, packages, catalog and sql variants", {
  fake_dep <- structure(
    list(
      jars = "ext.jar",
      packages = "org:ext:1.0",
      initializer = NULL,
      catalog = "ext.jar",
      repositories = "myrepo",
      dbplyr_sql_variant = list(
        scalar = list(my_fn = "translation"),
        aggregate = list(),
        window = list()
      )
    ),
    class = "spark_dependency"
  )

  with_mocked_bindings(
    spark_dependencies_from_extension = function(...) list(fake_dep),
    .package = "sparklyr",
    {
      # character catalog template ("%s" present)
      res <- spark_dependencies_from_extensions(
        numeric_version("3.5.0"),
        "2.12",
        extensions = list("ext"),
        config = list(sparklyr.extensions.catalog = "/catalog/%s")
      )
      expect_true("ext.jar" %in% res$jars)
      expect_true("org:ext:1.0" %in% res$packages)
      expect_equal(res$dbplyr_sql_variant$scalar$my_fn, "translation")
      expect_true(any(grepl("ext.jar", res$catalog_jars)))

      # default catalog (TRUE) exercises the "%s"-append + non-character branch
      res2 <- spark_dependencies_from_extensions(
        numeric_version("3.5.0"),
        "2.12",
        extensions = list("ext"),
        config = list()
      )
      expect_true(length(res2$catalog_jars) > 0)
    }
  )
})

# ---- sparklyr_jar_path ------------------------------------------------------

test_that("sparklyr_jar_path resolves shipped jars across versions", {
  # exact match
  expect_match(
    sparklyr_jar_path(numeric_version("3.5.0"), "2.12"),
    "sparklyr-3.5-2.12.jar"
  )
  # Spark 4.x maps to the master jar
  expect_match(
    sparklyr_jar_path(numeric_version("4.0.0")),
    "sparklyr-master-2.13.jar"
  )
  # scala default (<3.0 -> 2.12) + closest-version fallback (2.4-2.12 absent)
  expect_match(
    sparklyr_jar_path(numeric_version("2.4.0")),
    "sparklyr-2.4-2.11.jar"
  )
  # in-between version falls back to the closest lower shipped jar
  expect_match(
    sparklyr_jar_path(numeric_version("3.2.0"), "2.99"),
    "sparklyr-3.0-2.12.jar"
  )
})

test_that("sparklyr_jar_path honors the .test_on_spark_master override", {
  withr::defer(
    if (exists(".test_on_spark_master", envir = .GlobalEnv)) {
      rm(".test_on_spark_master", envir = .GlobalEnv)
    }
  )
  assign(".test_on_spark_master", TRUE, envir = .GlobalEnv)

  # no exact jar for scala 2.99 -> override picks the master jar
  expect_match(
    sparklyr_jar_path(numeric_version("2.4.0"), "2.99"),
    "sparklyr-master-2.13.jar"
  )
})

test_clear_cache()
