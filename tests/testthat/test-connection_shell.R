skip_connection("connection_shell")
skip_on_livy()
test_requires_version("3.0")
skip_databricks_connect()
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'spark_connect' can create a secondary connection", {
  sc2 <- spark_connect(master = "local", app_name = "other")
  spark_disconnect(sc2)

  succeed()
})

test_that("`spark_connect()` returns invisibly", {
  skip_on_arrow() # Why is this skipped on arrow?
  sc2 <- spark_connect(master = "local", app_name = "other")
  expect_identical(
    capture.output(spark_disconnect(sc2)),
    character(0)
  )
})

test_that("'spark_connect' can provide a 'spark_log'", {
  log <- capture.output({
    spark_log(sc)
  })

  expect_gte(length(log), 1)
})

test_that("'spark_connect' fails with bad configuration'", {
  config <- spark_config()

  config$sparklyr.shell.args <- c("--badargument")
  config$sparklyr.connect.timeout <- 3

  expect_error({
    spark_connect(
      master = "local",
      app_name = "bad_connection",
      config = config
    )
  })
})

test_that("'spark_session_id' generates different ids for different apps", {
  expect_true(
    spark_session_id(app_name = "foo", master = "local") !=
      spark_session_id(app_name = "bar", master = "local")
  )
})

test_that("'spark_session_id' generates same ids for same apps", {
  expect_equal(
    spark_session_id(app_name = "foo", master = "local"),
    spark_session_id(app_name = "foo", master = "local")
  )
})

test_that("'spark_session_random' generates different ids even with seeds", {
  expect_true(
    {
      set.seed(10)
      spark_session_random()
    } !=
      {
        set.seed(10)
        spark_session_random()
      }
  )
})

test_that("'spark_inspect' can enumerate information from the context", {
  result <- capture.output({
    sparklyr:::spark_inspect(spark_context(sc))
  })

  expect_gte(length(result), 100)
})

test_that("'spark_connect' can allow Hive support to be disabled", {
  version <- spark_version(sc)

  if (version >= "2.0.0") {
    expect_equal(get_spark_sql_catalog_implementation(sc), "hive")
  }

  # hive support is enabled by default
  expect_equal(sc$state$hive_support_enabled, TRUE)

  # create another connection with hive support disabled
  config <- spark_config()
  config$sparklyr.connect.enablehivesupport <- FALSE
  sc2 <- spark_connect(
    master = "local",
    app_name = "sparklyr_hive_support_disabled",
    config = config
  )

  if (version >= "2.0.0") {
    expect_equal(get_spark_sql_catalog_implementation(sc2), "in-memory")
  }

  expect_equal(sc2$state$hive_support_enabled, FALSE)
  spark_disconnect(sc2)

  # re-create another connection with hive support explicitly enabled
  config$sparklyr.connect.enablehivesupport <- TRUE
  sc2 <- spark_connect(
    master = "local",
    app_name = "sparklyr_hive_support_enabled",
    config = config
  )

  if (version >= "2.0.0") {
    expect_equal(get_spark_sql_catalog_implementation(sc2), "hive")
  }

  expect_equal(sc2$state$hive_support_enabled, TRUE)
  spark_disconnect(sc2)

  succeed()
})

test_that("spark_connection reports correct dbplyr edition", {
  dbplyr_version <- Sys.getenv("DBPLYR_VERSION")
  if (!identical(dbplyr_version, "") && dbplyr_version < "2") {
    skip("test case is not applicable for dbplyr 1.x")
  }

  expect_equal(
    dbplyr::dbplyr_edition(sc),
    ifelse(identical(Sys.getenv("DBPLYR_API_EDITION"), "1"), 1L, 2L)
  )
})

test_that("Abort shell returns expected output", {
  expect_error(
    abort_shell(
      output_file = tempfile(),
      error_file = tempfile(),
      message = "test",
      spark_submit_path = "",
      shell_args = ""
    )
  )
})

test_that("Misc tests", {
  expect_true(
    spark_connection_in_driver(testthat_spark_connection())
  )

  expect_equal(
    spark_disconnect("test"),
    0
  )

  expect_silent(
    spark_log_file(testthat_spark_connection())
  )
})

test_that("shell_connection_validate_config warns on the deprecated jars config", {
  expect_warning(
    cfg <- shell_connection_validate_config(list(spark.jars.default = "x.jar")),
    "deprecated"
  )
  expect_equal(cfg[["sparklyr.jars.default"]], "x.jar")
  expect_silent(shell_connection_validate_config(list()))
})

test_that("abort_shell surfaces log/error file contents and parameters", {
  out <- tempfile()
  writeLines(c("out line 1", "out line 2"), out)
  err <- tempfile()
  writeLines("err line 1", err)

  with_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    expect_error(
      abort_shell(
        message = "boom",
        spark_submit_path = "/bin/spark-submit",
        shell_args = c("--a", "--b"),
        output_file = out,
        error_file = err
      ),
      "out line 2"
    )
  )
})

test_that("spark_log handles a filter and an unavailable log", {
  f <- tempfile()
  writeLines(c("INFO ok", "ERROR boom"), f)
  sc_fake <- structure(
    list(output_file = f),
    class = c("spark_shell_connection", "spark_connection")
  )

  filtered <- spark_log.spark_shell_connection(sc_fake, filter = "ERROR")
  expect_s3_class(filtered, "spark_log")
  expect_true(all(grepl("ERROR", filtered)))

  with_mocked_bindings(
    file = function(...) stop("no log"),
    .package = "base",
    expect_equal(
      as.character(spark_log.spark_shell_connection(sc_fake)),
      "Spark log is not available."
    )
  )
})

test_that("print_jobj reports a detached jobj when the connection is closed", {
  sc_fake <- structure(
    list(),
    class = c("spark_shell_connection", "spark_connection")
  )
  with_mocked_bindings(
    connection_is_open = function(sc) FALSE,
    .package = "sparklyr",
    {
      out <- capture.output(
        print_jobj.spark_shell_connection(sc_fake, list(id = "7"))
      )
      expect_match(paste(out, collapse = " "), "detached")
    }
  )
})

test_that("shell j_invoke dispatch delegates to the core invoke helpers", {
  sc_fake <- structure(
    list(),
    class = c("spark_shell_connection", "spark_connection")
  )

  # j_invoke_method passes jObject = TRUE through to core_invoke_method
  with_mocked_bindings(
    core_invoke_method = function(sc, static, object, method, jObject, ...) {
      list(static = static, jObject = jObject, method = method)
    },
    .package = "sparklyr",
    {
      r <- j_invoke_method.spark_shell_connection(sc_fake, TRUE, "C", "m")
      expect_true(r$jObject)
    }
  )

  with_mocked_bindings(
    j_invoke_method = function(sc, static, object, method, ...) {
      list(static = static, method = method)
    },
    spark_connection = function(x) "sc",
    .package = "sparklyr",
    {
      expect_false(j_invoke.shell_jobj(list(), "m")$static)
      expect_equal(
        j_invoke_static.spark_shell_connection("sc", "C", "m")$method,
        "m"
      )
      expect_equal(
        j_invoke_new.spark_shell_connection("sc", "C")$method,
        "<init>"
      )
    }
  )
})

test_that("start_shell assembles --jars/--packages/--repositories args", {
  # batch = TRUE makes start_shell build the args + call spark-submit, then
  # return early (no gateway/socket). Mock spark_connect_gateway to NULL so it
  # takes the launch path, and capture the spark-submit args from system2.
  captured <- NULL
  home <- withr::local_tempdir()
  dir.create(file.path(home, "bin"))
  invisible(file.create(file.path(home, "bin", "spark-submit")))
  jar1 <- withr::local_tempfile(fileext = ".jar")
  invisible(file.create(jar1))

  with_mocked_bindings(
    spark_connect_gateway = function(...) NULL,
    spark_version_from_home = function(...) "3.5.0",
    spark_default_app_jar = function(...) {
      f <- tempfile(fileext = ".jar")
      file.create(f)
      f
    },
    spark_dependencies_from_extensions = function(...) {
      list(
        jars = character(),
        packages = "org.example:lib:1.0",
        repositories = "https://repo"
      )
    },
    .package = "sparklyr",
    with_mocked_bindings(
      system2 = function(command, args, ...) {
        captured <<- args
        0L
      },
      .package = "base",
      expect_null(start_shell(
        master = "local",
        spark_home = home,
        spark_version = "3.5.0",
        app_name = "t",
        config = list(),
        extensions = list(),
        jars = jar1,
        packages = "com.example:foo:2.0",
        environment = new.env(),
        batch = TRUE
      ))
    )
  )

  joined <- paste(captured, collapse = " ")
  expect_match(joined, "--jars")
  expect_match(joined, basename(jar1), fixed = TRUE)
  expect_match(joined, "--packages")
  expect_match(joined, "com.example:foo:2.0", fixed = TRUE)
  expect_match(joined, "--repositories")
})

test_that("start_shell errors on bad spark_home / missing spark-submit / version", {
  run <- function(
    spark_home,
    config = list(sparklyr.connect.app.jar = "x.jar"),
    app_jar_mock = function(...) "irrelevant.jar"
  ) {
    with_mocked_bindings(
      spark_connect_gateway = function(...) NULL,
      spark_version_from_home = function(...) "3.5.0",
      spark_default_app_jar = app_jar_mock,
      .package = "sparklyr",
      start_shell(
        master = "local",
        spark_home = spark_home,
        spark_version = "3.5.0",
        app_name = "t",
        config = config,
        extensions = list(),
        environment = new.env()
      )
    )
  }

  # empty spark_home (app jar supplied via config so we reach the nzchar guard)
  expect_error(run(""), "No spark_home")
  # spark_home points at a missing directory
  expect_error(run("/no/such/dir"), "not found")
  # real directory but no bin/spark-submit
  empty_home <- withr::local_tempdir()
  expect_error(run(empty_home), "Failed to find")
  # unsupported Spark version: spark_default_app_jar returns ""
  expect_error(
    run("/whatever", config = list(), app_jar_mock = function(...) ""),
    "does not support"
  )
})

test_that("shell_connection errors when SPARK_HOME is not set", {
  # non-local master skips the install-find/windows-prep branches, so we land on
  # the nzchar(spark_home) guard.
  with_mocked_bindings(
    validate_java_version = function(...) invisible(TRUE),
    .package = "sparklyr",
    expect_error(
      shell_connection(
        master = "spark://host:7077",
        spark_home = "",
        method = "",
        app_name = "t",
        version = NULL,
        hadoop_version = NULL,
        shell_args = NULL,
        config = list(),
        service = FALSE,
        remote = FALSE,
        extensions = list(),
        batch = NULL
      ),
      "SPARK_HOME is not set"
    )
  )
})

test_that("start_shell aborts after exhausting gateway connect attempts", {
  # routing gateway returns NULL (enter launch path); the retry-loop gateway
  # always errors, so after the attempts run out start_shell calls abort_shell.
  home <- withr::local_tempdir()
  dir.create(file.path(home, "bin"))
  invisible(file.create(file.path(home, "bin", "spark-submit")))

  with_mocked_bindings(
    spark_connect_gateway = function(..., isStarting = FALSE) {
      if (isStarting) stop("connection refused") else NULL
    },
    spark_version_from_home = function(...) "3.5.0",
    spark_default_app_jar = function(...) {
      f <- tempfile(fileext = ".jar")
      file.create(f)
      f
    },
    spark_dependencies_from_extensions = function(...) {
      list(
        jars = character(),
        packages = character(),
        repositories = character()
      )
    },
    .package = "sparklyr",
    with_mocked_bindings(
      system2 = function(...) 0L,
      Sys.sleep = function(...) invisible(NULL),
      .package = "base",
      expect_error(
        start_shell(
          master = "local",
          spark_home = home,
          spark_version = "3.5.0",
          app_name = "t",
          config = list(),
          extensions = list(),
          environment = new.env(),
          gateway_connect_attempts = 1
        ),
        "Failed while connecting to sparklyr"
      )
    )
  )
})

test_that("start_shell aborts when the backend socket cannot be opened", {
  # routing gateway returns a live-looking gatewayInfo, so we skip the launch
  # block and go straight to the socket setup, which we force to fail.
  gw_con <- textConnection("x")
  # start_shell's error handler closes the gateway; defer a guarded close so the
  # fixture is cleaned up regardless (tryCatch swallows the already-closed case).
  withr::defer(tryCatch(close(gw_con), error = function(e) NULL))
  gw <- list(backendPort = 9999, gateway = gw_con)
  with_mocked_bindings(
    spark_connect_gateway = function(...) gw,
    .package = "sparklyr",
    with_mocked_bindings(
      socketConnection = function(...) stop("refused"),
      Sys.sleep = function(...) invisible(NULL),
      .package = "base",
      expect_error(
        start_shell(
          master = "local",
          spark_home = "/x",
          spark_version = "3.5.0",
          app_name = "t",
          config = list(),
          extensions = list(),
          environment = new.env()
        ),
        "Failed to open connection to backend"
      )
    )
  )
})

test_that("initialize_connection aborts when backend setup fails", {
  sc <- structure(
    list(
      state = new.env(),
      output_file = tempfile(),
      app_name = "t",
      master = "local",
      config = list()
    ),
    class = c("spark_shell_connection", "spark_connection")
  )
  with_mocked_bindings(
    invoke_static = function(...) stop("no backend"),
    .package = "sparklyr",
    with_mocked_bindings(
      Sys.sleep = function(...) invisible(NULL),
      .package = "base",
      expect_error(
        initialize_connection.spark_shell_connection(sc),
        "Failed during initialize_connection"
      )
    )
  )
})

test_clear_cache()
