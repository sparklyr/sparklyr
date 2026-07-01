# ---------------------------------------------------------------------------
# Connection-free unit tests for the pure helpers in R/connection.R. These run
# without a live Spark session, so they are placed ABOVE the aborting
# skip_connection() call below. They exercise master/url predicates, version
# parsing, the in-memory connection registry, and the small validation/error
# branches via crafted inputs and `with_mocked_bindings`.
# ---------------------------------------------------------------------------

# A lightweight, fully in-memory connection object. It carries the
# `test_connection` class so that `connection_is_open()` dispatches to the
# method in connection_test.R and reads `state$open`, which we control.
fake_scon <- function(
  master = "local",
  open = TRUE,
  method = NULL,
  app_name = NULL,
  config = list()
) {
  st <- new.env()
  st$open <- open
  structure(
    list(
      master = master,
      method = method,
      app_name = app_name,
      config = config,
      state = st
    ),
    class = c("test_connection", "spark_connection")
  )
}

test_that("spark_master_is_local() matches only local master strings", {
  expect_true(spark_master_is_local("local"))
  expect_true(spark_master_is_local("local[4]"))
  expect_true(spark_master_is_local("local[*]"))
  expect_false(spark_master_is_local("yarn"))
  expect_false(spark_master_is_local("spark://HOST:PORT"))
  expect_false(spark_master_is_local("local-cluster"))
  # NULL master is tolerated and reported as FALSE
  expect_false(spark_master_is_local(NULL))
})

test_that("spark_master_is_gateway() detects the sparklyr:// scheme", {
  expect_true(spark_master_is_gateway("sparklyr://localhost:8880/0"))
  expect_false(spark_master_is_gateway("local"))
  expect_false(spark_master_is_gateway("yarn"))
})

test_that("spark_master_is_yarn_cluster() honors master and deploy-mode", {
  expect_true(spark_master_is_yarn_cluster("yarn-cluster", list()))
  expect_true(spark_master_is_yarn_cluster("YARN-CLUSTER", list()))
  # yarn + cluster deploy-mode counts as a yarn cluster
  expect_true(spark_master_is_yarn_cluster(
    "yarn",
    list(`sparklyr.shell.deploy-mode` = "cluster")
  ))
  # plain yarn (client deploy) does not
  expect_false(spark_master_is_yarn_cluster("yarn", list()))
  expect_false(spark_master_is_yarn_cluster("local", list()))
})

test_that("spark_connection_is_yarn() recognizes yarn variants", {
  expect_true(spark_connection_is_yarn(fake_scon(master = "yarn")))
  expect_true(spark_connection_is_yarn(fake_scon(master = "yarn-client")))
  expect_true(spark_connection_is_yarn(fake_scon(master = "yarn-cluster")))
  expect_true(spark_connection_is_yarn(fake_scon(master = "YARN")))
  expect_false(spark_connection_is_yarn(fake_scon(master = "local")))
})

test_that("spark_connection_is_yarn_client() distinguishes client vs cluster", {
  expect_true(spark_connection_is_yarn_client(fake_scon(
    master = "yarn-client"
  )))
  # bare yarn with no cluster deploy-mode is treated as client
  expect_true(spark_connection_is_yarn_client(fake_scon(master = "yarn")))
  # bare yarn with cluster deploy-mode is NOT a client
  expect_false(spark_connection_is_yarn_client(
    fake_scon(
      master = "yarn",
      config = list(`sparklyr.shell.deploy-mode` = "cluster")
    )
  ))
  expect_false(spark_connection_is_yarn_client(fake_scon(
    master = "yarn-cluster"
  )))
  expect_false(spark_connection_is_yarn_client(fake_scon(master = "local")))
})

test_that("spark_connection_is_local() excludes databricks-connect", {
  expect_true(spark_connection_is_local(fake_scon(master = "local")))
  expect_true(spark_connection_is_local(fake_scon(master = "local[2]")))
  expect_false(spark_connection_is_local(fake_scon(master = "yarn")))
  # local master but databricks-connect method -> not "local"
  expect_false(spark_connection_is_local(
    fake_scon(master = "local", method = "databricks-connect")
  ))
})

test_that("spark_connection_in_driver() is TRUE for local and yarn-client", {
  expect_true(spark_connection_in_driver(fake_scon(master = "local")))
  expect_true(spark_connection_in_driver(fake_scon(master = "yarn-client")))
  expect_false(spark_connection_in_driver(fake_scon(master = "yarn-cluster")))
})

test_that("spark_version_numeric() strips non-numeric decorations", {
  expect_equal(spark_version_numeric("3.5.0"), numeric_version("3.5.0"))
  expect_equal(spark_version_numeric("3.5.0-preview"), numeric_version("3.5.0"))
  expect_equal(spark_version_numeric("v2.4.7"), numeric_version("2.4.7"))
})

test_that("spark_default_app_jar() resolves a shipped jar and defaults version", {
  jar <- spark_default_app_jar("3.5.0")
  expect_true(grepl("sparklyr-3\\.5-.*\\.jar$", jar))
  expect_true(file.exists(jar))

  # version = NULL falls back to .spark_default_version (1.6.2) without error
  expect_type(spark_default_app_jar(NULL), "character")
})

test_that("spark_master_local_cores() injects cores only for plain local", {
  cfg <- list(sparklyr.cores.local = 4)
  expect_equal(spark_master_local_cores("local", cfg), "local[4]")
  # already-decorated local is left untouched (regex only matches "local")
  expect_equal(spark_master_local_cores("local[2]", cfg), "local[2]")
  expect_equal(spark_master_local_cores("yarn", cfg), "yarn")
  # no cores configured -> master unchanged
  expect_equal(spark_master_local_cores("local", list()), "local")
})

test_that("spark_config_shell_args() flattens sparklyr.shell.* into --flag pairs", {
  config <- list(
    sparklyr.shell.packages = c("a", "b"),
    `sparklyr.shell.driver-memory` = "2g",
    spark.some.other = "ignored"
  )
  args <- spark_config_shell_args(config, "local")
  args <- unlist(args)
  # packages collapsed to a comma-separated value behind --packages
  expect_true("--packages" %in% args)
  expect_true("a,b" %in% args)
  # other sparklyr.shell.* entries surface as their own --flag value pairs
  expect_true("--driver-memory" %in% args)
  expect_true("2g" %in% args)
  # non sparklyr.shell.* keys are not emitted
  expect_false("--some.other" %in% args)
})

test_that("as_spark_method() builds a dispatch-only S3 object", {
  m <- as_spark_method("livy")
  expect_s3_class(m, "spark_method_livy")
  expect_identical(unclass(m), list())
})

test_that("no_databricks_guid() reflects the global DATABRICKS_GUID", {
  # Ensure clean baseline
  if (exists("DATABRICKS_GUID", envir = .GlobalEnv)) {
    rm("DATABRICKS_GUID", envir = .GlobalEnv)
  }
  expect_true(no_databricks_guid())

  withr::defer({
    if (exists("DATABRICKS_GUID", envir = .GlobalEnv)) {
      rm("DATABRICKS_GUID", envir = .GlobalEnv)
    }
  })
  assign("DATABRICKS_GUID", "abc", envir = .GlobalEnv)
  expect_false(no_databricks_guid())
})

test_that("connection_is_open() dispatches and reads test_connection state", {
  expect_true(connection_is_open(fake_scon(open = TRUE)))
  expect_false(connection_is_open(fake_scon(open = FALSE)))
})

test_that("in-memory connection registry adds, finds, and removes connections", {
  # Snapshot and restore the shared registry so we don't leak fakes.
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  a <- fake_scon(master = "local[1]", app_name = "appA", method = "test")
  b <- fake_scon(
    master = "local[2]",
    app_name = "appB",
    method = "test",
    open = FALSE
  )

  spark_connections_add(a)
  spark_connections_add(b)
  expect_length(spark_connection_instances(), 2)

  # find filters on open + matching master/app_name/method
  found <- spark_connection_find(
    master = "local[1]",
    app_name = "appA",
    method = "test"
  )
  expect_length(found, 1)
  expect_identical(found[[1]]$master, "local[1]")

  # closed connection (b) is not returned by find even on an exact match
  expect_length(
    spark_connection_find(
      master = "local[2]",
      app_name = "appB",
      method = "test"
    ),
    0
  )

  # NULL filters match anything that is open
  expect_length(spark_connection_find(), 1)

  # remove drops the matching master entry
  spark_connections_remove(a)
  expect_length(spark_connection_instances(), 1)
  expect_identical(spark_connection_instances()[[1]]$master, "local[2]")
})

test_that("spark_connection_instances() initializes an empty list when unset", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- NULL
  expect_identical(spark_connection_instances(), list())
})

test_that("spark_disconnect.character() returns count of disconnected matches", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  # No matching registered connection -> 0 disconnected
  expect_equal(spark_disconnect("spark://nope:7077"), 0)

  # A registered, open connection whose master matches (cores stripped) -> 1
  conn <- fake_scon(master = "local[4]", app_name = "appX")
  spark_connections_add(conn)
  n <- spark_disconnect("local", master = "local")
  expect_equal(n, 1)
  # spark_disconnect.test_connection flips the open flag
  expect_false(conn$state$open)
})

test_that("spark_disconnect_all() disconnects every open connection", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  open1 <- fake_scon(master = "local[1]")
  open2 <- fake_scon(master = "local[2]")
  closed <- fake_scon(master = "local[3]", open = FALSE)
  spark_connections_add(open1)
  spark_connections_add(open2)
  spark_connections_add(closed)

  expect_equal(spark_disconnect_all(), 2)
  expect_false(open1$state$open)
  expect_false(open2$state$open)
})

test_that("spark_log_file() errors when the connection is closed", {
  expect_error(
    spark_log_file(fake_scon(open = FALSE)),
    "not open"
  )
})

test_that("spark_connect_method.default() rejects unsupported methods", {
  # spark_config_shell_args is exercised first; stub it so we reach the
  # unsupported-method branch deterministically without touching real config.
  with_mocked_bindings(
    spark_config_shell_args = function(config, master) list(),
    .package = "sparklyr",
    expect_error(
      spark_connect_method(
        x = as_spark_method("bogus"),
        method = "bogus",
        master = "local",
        spark_home = "",
        config = list(),
        app_name = "sparklyr",
        version = NULL,
        extensions = list(),
        scala_version = NULL
      ),
      "Unsupported connection method"
    )
  )
})

test_that("spark_connection_is_open() delegates to connection_is_open()", {
  expect_true(spark_connection_is_open(fake_scon(open = TRUE)))
  expect_false(spark_connection_is_open(fake_scon(open = FALSE)))
})

test_that("requireNamespace2() forwards to base requireNamespace", {
  expect_true(requireNamespace2("stats", quietly = TRUE))
  expect_false(requireNamespace2("nonexistentpkgxyz123", quietly = TRUE))
})

test_that("spark_connect() aborts for pysparklyr methods when it is missing", {
  # The databricks_connect / spark_connect methods are provided by pysparklyr;
  # with the package "absent" spark_connect must abort before doing any work.
  with_mocked_bindings(
    requireNamespace2 = function(...) FALSE,
    .package = "sparklyr",
    {
      expect_error(
        spark_connect(master = "local", method = "spark_connect"),
        "pysparklyr"
      )
      expect_error(
        spark_connect(master = "local", method = "databricks_connect"),
        "pysparklyr"
      )
    }
  )
})

# ---------------------------------------------------------------------------
# spark_connect() orchestration. The real body negotiates master/method,
# short-circuits on a re-used connection, and otherwise hands off to
# spark_connect_method(). We drive the *resolution logic* with all the live
# machinery stubbed out: spark_connect_method captures the master/method it is
# handed, and the post-connect helpers are neutralized so nothing touches a real
# JVM. This covers the branch selection without a live session.
# ---------------------------------------------------------------------------

# Build a spark_connect() caller that stubs the connection machinery and returns
# the (master, method) that spark_connect resolved and passed downstream.
make_connect_capture <- function(captured_env) {
  function(...) {
    captured_env$value <- NULL
    with_mocked_bindings(
      spark_connect_method = function(x, method, master, ...) {
        captured_env$value <- list(master = master, method = method)
        structure(
          list(
            master = master,
            method = method,
            # open = FALSE so the real reg.finalizer registered by spark_connect()
            # sees a closed connection at exit and skips its Sys.sleep() guard.
            state = list(open = FALSE),
            extensions = list()
          ),
          class = c("test_connection", "spark_connection")
        )
      },
      initialize_connection = function(scon) scon,
      initialize_method = function(method, scon) scon,
      register_mapping_tables = function() invisible(NULL),
      spark_web = function(scon, ...) NULL,
      spark_ide_connection_open = function(...) invisible(NULL),
      spark_connections_add = function(sc) invisible(NULL),
      .package = "sparklyr",
      spark_connect(...)
    )
    captured_env$value
  }
}

test_that("spark_connect() resolves master/method then routes downstream", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  cap <- new.env()
  do_connect <- make_connect_capture(cap)

  # master overridden via config$sparklyr.connect.master
  res <- do_connect(
    master = "local",
    method = "test",
    config = list(sparklyr.connect.master = "local[7]")
  )
  expect_equal(res$master, "local[7]")

  # qubole with a missing master defaults master + spark_home
  res <- do_connect(method = "qubole", config = list())
  expect_equal(res$master, "yarn-client")
  expect_equal(res$method, "qubole")

  # missing master with no spark.master leaves master NULL (default shell)
  res <- do_connect(config = list())
  expect_null(res$master)
  expect_equal(res$method, "shell")

  # the documentation example master switches to the in-memory test method
  res <- do_connect(master = "spark://HOST:PORT", config = list())
  expect_equal(res$method, "test")

  # on a Databricks cluster (a DATABRICKS_GUID exists) method = "databricks" is
  # NOT remapped to databricks-connect, so a missing master defaults to
  # "databricks".
  withr::defer({
    if (exists("DATABRICKS_GUID", envir = .GlobalEnv)) {
      rm("DATABRICKS_GUID", envir = .GlobalEnv)
    }
  })
  assign("DATABRICKS_GUID", "abc", envir = .GlobalEnv)
  res <- do_connect(method = "databricks", config = list())
  expect_equal(res$master, "databricks")
  expect_equal(res$method, "databricks")
})

test_that("spark_connect() maps databricks -> databricks-connect off-cluster", {
  # No DATABRICKS_GUID in the global env means method = "databricks" refers to
  # Databricks Connect, so master is forced local and the client-type property
  # is set after connecting.
  if (exists("DATABRICKS_GUID", envir = .GlobalEnv)) {
    rm("DATABRICKS_GUID", envir = .GlobalEnv)
  }

  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  captured <- NULL
  prop <- NULL
  with_mocked_bindings(
    spark_connect_method = function(x, method, master, ...) {
      captured <<- list(master = master, method = method)
      structure(
        list(
          master = master,
          method = method,
          state = list(open = FALSE),
          extensions = list()
        ),
        class = c("test_connection", "spark_connection")
      )
    },
    initialize_connection = function(scon) scon,
    initialize_method = function(method, scon) scon,
    register_mapping_tables = function() invisible(NULL),
    spark_web = function(scon, ...) NULL,
    spark_ide_connection_open = function(...) invisible(NULL),
    spark_connections_add = function(sc) invisible(NULL),
    spark_context = function(scon) "ctx",
    invoke = function(jobj, method, ...) {
      prop <<- method
      NULL
    },
    .package = "sparklyr",
    spark_connect(method = "databricks", config = list())
  )
  expect_equal(captured$method, "databricks-connect")
  expect_equal(captured$master, "local")
  expect_equal(prop, "setLocalProperty")
})

test_that("spark_connect() clears the apply bundle and runs initializers", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  bundle <- withr::local_tempdir()
  ran <- FALSE
  scon_obj <- structure(
    list(
      master = "local",
      method = "shell",
      state = list(open = FALSE),
      extensions = list(initializers = list(function(scon) ran <<- TRUE))
    ),
    class = c("test_connection", "spark_connection")
  )

  with_mocked_bindings(
    spark_apply_bundle_path = function() bundle,
    spark_connect_method = function(...) scon_obj,
    initialize_connection = function(scon) scon,
    initialize_method = function(method, scon) scon,
    register_mapping_tables = function() invisible(NULL),
    spark_web = function(scon, ...) NULL,
    spark_ide_connection_open = function(...) invisible(NULL),
    spark_connections_add = function(sc) invisible(NULL),
    .package = "sparklyr",
    spark_connect(master = "local", method = "test", config = list())
  )

  expect_false(dir.exists(bundle)) # pre-existing bundle was unlinked
  expect_true(ran) # extension initializer was invoked
})

test_that("spark_connect() re-uses an open connection with matching params", {
  old <- sparkConnectionsEnv$instances
  withr::defer(sparkConnectionsEnv$instances <- old)
  sparkConnectionsEnv$instances <- list()

  existing <- fake_scon(
    master = "local",
    app_name = "sparklyr",
    method = "shell"
  )
  spark_connections_add(existing)

  expect_message(
    reused <- spark_connect(
      master = "local",
      app_name = "sparklyr",
      method = "shell",
      config = list()
    ),
    "Re-using existing"
  )
  expect_identical(reused, existing)
})

test_that("spark_connect_method.default() routes each method to its constructor", {
  fake <- structure(
    list(state = list(), extensions = list()),
    class = c("test_connection", "spark_connection")
  )
  base_args <- list(
    master = "local",
    spark_home = "",
    config = list(),
    app_name = "sparklyr",
    version = NULL,
    extensions = list(),
    scala_version = NULL
  )

  with_mocked_bindings(
    spark_config_shell_args = function(config, master) list(),
    shell_connection = function(...) fake,
    livy_connection = function(...) "livy",
    databricks_connection = function(...) "dbx",
    synapse_connection = function(...) "syn",
    .package = "sparklyr",
    {
      # qubole goes through shell_connection and is tagged with its method
      q <- do.call(
        spark_connect_method,
        c(list(x = as_spark_method("qubole"), method = "qubole"), base_args)
      )
      expect_equal(q$method, "qubole")

      expect_equal(
        do.call(
          spark_connect_method,
          c(list(x = as_spark_method("livy"), method = "livy"), base_args)
        ),
        "livy"
      )
      expect_equal(
        do.call(
          spark_connect_method,
          c(
            list(x = as_spark_method("databricks"), method = "databricks"),
            base_args
          )
        ),
        "dbx"
      )
      expect_equal(
        do.call(
          spark_connect_method,
          c(list(x = as_spark_method("synapse"), method = "synapse"), base_args)
        ),
        "syn"
      )
    }
  )
})

test_that("spark_inspect() returns the jobj when the connection is closed", {
  fake_jobj <- structure(list(), class = "spark_jobj")
  with_mocked_bindings(
    print = function(x, ...) invisible(x),
    .package = "base",
    {
      result <- with_mocked_bindings(
        spark_connection = function(x, ...) fake_scon(open = FALSE),
        .package = "sparklyr",
        spark_inspect(fake_jobj)
      )
      expect_identical(result, fake_jobj)
    }
  )
})

skip_connection("connection")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

call_sparkr <- function(method, ...) {
  get(method, envir = asNamespace("SparkR"))(...)
}

test_that("sparklyr_get_backend_port() returns the live backend port", {
  port <- sparklyr_get_backend_port(sc)
  expect_true(is.numeric(port))
  expect_length(port, 1)
  expect_gt(port, 0)
})

test_that("gateway connection fails with invalid session", {
  expect_error(
    spark_connect(master = "sparklyr://localhost:8880/0")
  )
})

test_that("can connect to an existing session via gateway", {
  gw <- spark_connect(
    master = paste0("sparklyr://localhost:8880/", sc$sessionId)
  )
  expect_equal(spark_context(gw)$backend, spark_context(sc)$backend)
})

test_that("sparklyr gateway for Synapse should be configured properly", {
  skip_unless_synapse_connect()
  connector <- invoke_static(
    sc,
    "org.apache.spark.sparklyr.SparklyrConnector",
    "getOrCreate"
  )
  expect_false(is.null(connector))
  gateway_url <- invoke_method(sc, FALSE, connector, "getUri")
  expect_true(grepl("sparklyr://[^:]+:\\d{1,5}", gateway_url))
})

test_that("sparklyr spark session should be configured properly", {
  skip_unless_synapse_connect()
  expect_equal(
    call_sparkr("callJMethod", call_sparkr("sparkR.session"), "hashCode"),
    invoke_method(sc, FALSE, sc$state$hive_context, "hashCode")
  )
})

test_clear_cache()
