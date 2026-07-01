# ---- Connection-free tests --------------------------------------------------
# These exercise the parts of the Kubernetes connector that do not require a
# live k8s cluster, kubectl, or spark-submit. They are placed *above* the
# skip_connection() gate, which otherwise aborts the file when no Spark
# connection is available.
#
# Note: the rstudioapi-based helpers (spark_config_kubernetes_terminal_id,
# spark_config_kubernetes_forward_init_terminal, and the Windows branch of
# spark_config_kubernetes_forward_cleanup) call rstudioapi with explicit
# `::` qualification, which with_mocked_bindings cannot intercept, so they
# are intentionally left uncovered here.

test_that("spark_config_kubernetes_forward_init runs kubectl port-forward", {
  slept <- NULL
  forwarded <- NULL
  with_mocked_bindings(
    Sys.sleep = function(time) {
      slept <<- time
      invisible(NULL)
    },
    system2 = function(command, args, ...) {
      forwarded <<- list(command = command, args = args, dots = list(...))
      invisible(0)
    },
    .package = "base",
    spark_config_kubernetes_forward_init(
      driver = "spark-driver",
      timeout = 0,
      ports = c("8880:8880", "4040:4040")
    )
  )

  expect_equal(slept, 0)
  expect_equal(forwarded$command, "kubectl")
  expect_equal(
    forwarded$args,
    c("port-forward", "spark-driver", "8880:8880", "4040:4040")
  )
  # system2 is invoked with wait = FALSE so the forward runs in the background
  expect_false(forwarded$dots$wait)
})

test_that("spark_config_kubernetes_forward_init_message prints kubectl command", {
  msgs <- testthat::capture_messages(
    spark_config_kubernetes_forward_init_message(
      driver = "spark-driver",
      timeout = 0,
      ports = c("8880:8880", "4040:4040")
    )
  )
  expect_match(msgs, "Please enable port forwarding", all = FALSE)
  expect_match(
    msgs,
    "kubectl port-forward spark-driver 8880:8880 4040:4040",
    all = FALSE,
    fixed = TRUE
  )
})

test_that("spark_config_kubernetes_forward_cleanup pkills kubectl on non-Windows", {
  skip_on_os("windows")
  killed <- NULL
  with_mocked_bindings(
    system2 = function(command, args, ...) {
      killed <<- list(command = command, args = args)
      invisible(0)
    },
    .package = "base",
    spark_config_kubernetes_forward_cleanup(driver = "spark-driver")
  )

  expect_equal(killed$command, "pkill")
  expect_equal(killed$args, "kubectl")
})

test_that("spark_config_kubernetes builds forward submit/disconnect functions", {
  skip_on_os("windows")
  config <- spark_config_kubernetes(
    master = "k8s://https://192.168.99.100:8443",
    version = "3.0",
    driver = "spark-driver",
    forward = TRUE,
    fix_config = FALSE
  )

  # forward = TRUE wires up both lifecycle callbacks
  expect_type(config$sparklyr.connect.aftersubmit, "closure")
  expect_type(config$sparklyr.connect.ondisconnect, "closure")

  # the submit callback forwards the doubled-up ports through kubectl
  forwarded <- NULL
  with_mocked_bindings(
    Sys.sleep = function(time) invisible(NULL),
    system2 = function(command, args, ...) {
      forwarded <<- args
      invisible(0)
    },
    .package = "base",
    config$sparklyr.connect.aftersubmit()
  )
  expect_equal(
    forwarded,
    c("port-forward", "spark-driver", "8880:8880", "8881:8881", "4040:4040")
  )

  # the disconnect callback cleans up the forward
  killed <- NULL
  with_mocked_bindings(
    system2 = function(command, args, ...) {
      killed <<- command
      invisible(0)
    },
    .package = "base",
    config$sparklyr.connect.ondisconnect()
  )
  expect_equal(killed, "pkill")
})

test_that("spark_config_kubernetes adds executor and conf entries", {
  config <- spark_config_kubernetes(
    master = "k8s://https://192.168.99.100:8443",
    version = "3.0",
    driver = "spark-driver",
    forward = FALSE,
    fix_config = FALSE,
    executors = 4,
    conf = list("spark.kubernetes.namespace" = "sparklyr-ns")
  )

  expect_true(
    "spark.executor.instances=4" %in% config$sparklyr.shell.conf
  )
  expect_true(
    "spark.kubernetes.namespace=sparklyr-ns" %in% config$sparklyr.shell.conf
  )
})

test_that("spark_config_kubernetes fixes spark-defaults.conf when fix_config = TRUE", {
  conf_dir <- withr::local_tempdir()
  defaults <- file.path(conf_dir, "spark-defaults.conf")
  writeLines(
    c(
      "spark.local.dir /tmp/spark",
      "spark.sql.warehouse.dir /tmp/warehouse",
      "spark.executor.memory 2g"
    ),
    defaults
  )

  with_mocked_bindings(
    spark_install_find = function(...) list(sparkConfDir = conf_dir),
    .package = "sparklyr",
    spark_config_kubernetes(
      master = "k8s://https://192.168.99.100:8443",
      version = "3.0",
      driver = "spark-driver",
      forward = FALSE,
      fix_config = TRUE
    )
  )

  fixed <- readLines(defaults)
  # the local.dir and warehouse.dir lines get commented out; others untouched
  expect_true("# spark.local.dir /tmp/spark" %in% fixed)
  expect_true("# spark.sql.warehouse.dir /tmp/warehouse" %in% fixed)
  expect_true("spark.executor.memory 2g" %in% fixed)
})

# ---- Connection-gated tests -------------------------------------------------

skip_connection("connection_kubernetes")
skip_on_livy()
skip_on_arrow_devel()

test_that("spark_kubernetes_config can generate correct config", {
  expect_equal(
    spark_config_kubernetes(
      master = "k8s://https://192.168.99.100:8443",
      version = "3.0",
      driver = "spark-driver",
      forward = FALSE,
      fix_config = FALSE
    ),
    list(
      spark.master = "k8s://https://192.168.99.100:8443",
      sparklyr.shell.master = "k8s://https://192.168.99.100:8443",
      "sparklyr.shell.deploy-mode" = "cluster",
      sparklyr.gateway.remote = TRUE,
      sparklyr.shell.name = "sparklyr",
      sparklyr.shell.class = "sparklyr.Shell",
      sparklyr.connect.timeout = 120,
      sparklyr.web.spark = "http://localhost:4040",
      sparklyr.shell.conf = c(
        "spark.kubernetes.container.image=spark:sparklyr",
        "spark.kubernetes.driver.pod.name=spark-driver",
        "spark.kubernetes.authenticate.driver.serviceAccountName=spark"
      ),
      sparklyr.gateway.routing = FALSE,
      sparklyr.app.jar = "local:///opt/sparklyr/sparklyr-3.0-2.12.jar",
      sparklyr.connect.aftersubmit = NULL,
      sparklyr.connect.ondisconnect = NULL,
      spark.home = spark_home_dir()
    )
  )
})

test_clear_cache()
