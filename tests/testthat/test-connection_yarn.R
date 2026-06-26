# ---------------------------------------------------------------------------
# Local, connection-free tests for YARN cluster helpers. These read the
# `data/yarn-site.xml` fixture for the conf-property logic and mock httr / the
# internal helpers for everything that would otherwise hit a live YARN resource
# manager. The live UI test is gated below by skip_connection().
# ---------------------------------------------------------------------------

yarn_conf_dir <- function() test_path("data")

test_that("spark_yarn_get_conf_property() requires a conf dir and yarn-site.xml", {
  withr::local_envvar(c(YARN_CONF_DIR = NA, HADOOP_CONF_DIR = NA))
  expect_error(
    spark_yarn_get_conf_property("anything"),
    "YARN_CONF_DIR or HADOOP_CONF_DIR"
  )
  expect_null(spark_yarn_get_conf_property("anything", fails = FALSE))

  # conf dir set, but no yarn-site.xml inside it
  empty <- withr::local_tempdir()
  withr::local_envvar(c(YARN_CONF_DIR = empty))
  expect_error(spark_yarn_get_conf_property("anything"), "yarn-site.xml")
  expect_null(spark_yarn_get_conf_property("anything", fails = FALSE))
})

test_that("spark_yarn_get_conf_property() reads values and expands variables", {
  withr::local_envvar(c(YARN_CONF_DIR = yarn_conf_dir()))

  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.nonexistent"),
    character()
  )
  expect_equal(spark_yarn_get_conf_property("yarn.resourcemanager.empty"), "")
  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.port"),
    "8032"
  )
  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.host"),
    "invalidhost123.com"
  )
  # single variable substitution
  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.hostname"),
    "invalidhost123.com"
  )
  # nested variable substitution
  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.address"),
    "invalidhost123.com:8032"
  )
})

test_that("spark_yarn_get_conf_property() falls back to HADOOP_CONF_DIR", {
  withr::local_envvar(c(
    YARN_CONF_DIR = NA,
    HADOOP_CONF_DIR = yarn_conf_dir()
  ))
  expect_equal(
    spark_yarn_get_conf_property("yarn.resourcemanager.port"),
    "8032"
  )
})

test_that("spark_yarn_cluster_get_protocol() picks https only when configured", {
  with_mocked_bindings(
    spark_yarn_cluster_get_conf_property = function(...) "HTTPS_ONLY",
    .package = "sparklyr",
    expect_equal(spark_yarn_cluster_get_protocol(), "https")
  )
  with_mocked_bindings(
    spark_yarn_cluster_get_conf_property = function(...) character(),
    .package = "sparklyr",
    expect_equal(spark_yarn_cluster_get_protocol(), "http")
  )
})

test_that("spark_yarn_cluster_resource_manager_is_online() maps responses to TRUE/FALSE", {
  with_mocked_bindings(
    spark_yarn_cluster_get_protocol = function() "http",
    .package = "sparklyr",
    {
      # reachable, no http error -> TRUE
      with_mocked_bindings(
        GET = function(...) "resp",
        http_error = function(...) FALSE,
        .package = "httr",
        expect_true(spark_yarn_cluster_resource_manager_is_online("host:8088"))
      )

      # http error -> warns, FALSE
      with_mocked_bindings(
        GET = function(...) "resp",
        http_error = function(...) TRUE,
        status_code = function(...) 500L,
        .package = "httr",
        expect_warning(
          expect_false(
            spark_yarn_cluster_resource_manager_is_online("host:8088")
          ),
          "Failed to open"
        )
      )

      # GET throws -> warns, FALSE
      with_mocked_bindings(
        GET = function(...) stop("boom"),
        .package = "httr",
        expect_warning(
          expect_false(
            spark_yarn_cluster_resource_manager_is_online("host:8088")
          ),
          "Failed to open"
        )
      )
    }
  )
})

test_that("spark_yarn_cluster_get_resource_manager_webapp() resolves an HA webapp", {
  withr::local_envvar(c(YARN_CONF_DIR = yarn_conf_dir()))

  # an online RM candidate is selected
  with_mocked_bindings(
    spark_yarn_cluster_resource_manager_is_online = function(...) TRUE,
    .package = "sparklyr",
    expect_match(
      spark_yarn_cluster_get_resource_manager_webapp(),
      "invalidhost123.com"
    )
  )

  # no RM online under HA -> error
  with_mocked_bindings(
    spark_yarn_cluster_resource_manager_is_online = function(...) FALSE,
    .package = "sparklyr",
    expect_error(
      spark_yarn_cluster_get_resource_manager_webapp(),
      "Failed to find online resource manager"
    )
  )
})

test_that("spark_yarn_cluster_get_resource_manager_webapp() handles non-HA fallback", {
  lookup <- function(values) {
    function(property) values[[property]] %||% character()
  }

  # non-HA: webapp.address absent -> fall back to resourcemanager.address:8088
  with_mocked_bindings(
    spark_yarn_cluster_get_conf_property = lookup(list(
      "yarn.resourcemanager.ha.enabled" = "false",
      "yarn.resourcemanager.webapp.address" = character(),
      "yarn.resourcemanager.address" = "rmhost:8032"
    )),
    .package = "sparklyr",
    expect_equal(
      spark_yarn_cluster_get_resource_manager_webapp(),
      "rmhost:8088"
    )
  )

  # non-HA: neither webapp.address nor resourcemanager.address -> error
  with_mocked_bindings(
    spark_yarn_cluster_get_conf_property = lookup(list(
      "yarn.resourcemanager.ha.enabled" = "false"
    )),
    .package = "sparklyr",
    expect_error(
      spark_yarn_cluster_get_resource_manager_webapp(),
      "Failed to retrieve"
    )
  )
})

test_that("spark_yarn_cluster_get_app_property() returns values and reports gaps", {
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(app = list(state = "RUNNING")),
    .package = "httr",
    expect_equal(
      spark_yarn_cluster_get_app_property("host", "app_1", "state"),
      "RUNNING"
    )
  )

  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(app = list(state = "RUNNING")),
    .package = "httr",
    expect_error(
      spark_yarn_cluster_get_app_property("host", "app_1", "missing"),
      "Failed to retrieve"
    )
  )
})

test_that("spark_yarn_cluster_while_app() polls until the condition is false", {
  calls <- 0
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(app = list(state = "RUNNING")),
    .package = "httr",
    {
      spark_yarn_cluster_while_app("host", "app_1", 30, function(app) {
        calls <<- calls + 1
        FALSE # stop immediately
      })
    }
  )
  expect_equal(calls, 1)
})

test_that("spark_yarn_cluster_get_app_id() finds, rejects duplicates, and times out", {
  app <- function(name = "sparklyr-app", id = "app_1", user = "tester") {
    list(list(name = name, id = id, user = user))
  }

  # found by prefix (byname disabled)
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(apps = list(app())),
    .package = "httr",
    expect_equal(
      spark_yarn_cluster_get_app_id(
        list(sparklyr.yarn.cluster.lookup.byname = FALSE),
        0,
        "host"
      ),
      "app_1"
    )
  )

  # found by user (the default by-name lookup, using the USER env var)
  me <- Sys.getenv("USER")
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(apps = list(app(user = me))),
    .package = "httr",
    expect_equal(
      spark_yarn_cluster_get_app_id(
        list(sparklyr.yarn.cluster.lookup.byname = TRUE),
        0,
        "host"
      ),
      "app_1"
    )
  )

  # multiple matches -> error
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(apps = list(app(), app(id = "app_2"))),
    .package = "httr",
    expect_error(
      spark_yarn_cluster_get_app_id(
        list(sparklyr.yarn.cluster.lookup.byname = FALSE),
        0,
        "host"
      ),
      "Multiple sparklyr apps"
    )
  )

  # nothing found before the (zero) timeout -> error
  with_mocked_bindings(
    GET = function(...) "resp",
    content = function(...) list(apps = list()),
    .package = "httr",
    expect_error(
      spark_yarn_cluster_get_app_id(
        list(
          sparklyr.yarn.cluster.lookup.byname = FALSE,
          sparklyr.yarn.cluster.start.timeout = 0
        ),
        0,
        "host"
      ),
      "Failed to retrieve new sparklyr"
    )
  )
})

test_that("spark_yarn_cluster_get_gateway() walks the accepted-state flow", {
  base_mocks <- function(state, body) {
    with_mocked_bindings(
      spark_yarn_cluster_get_resource_manager_webapp = function() "rmhost:8088",
      spark_yarn_cluster_get_protocol = function() "http",
      spark_yarn_cluster_get_app_id = function(...) "app_1",
      spark_yarn_cluster_while_app = function(rm, appId, wait, condition) {
        # exercise the inline state/host-address condition closures
        condition(list(state = "ACCEPTED", amHostHttpAddress = "amhost:8042"))
        invisible(NULL)
      },
      spark_yarn_cluster_get_app_property = function(rm, appId, property, ...) {
        if (property == "state") state else "amhost:8042"
      },
      .package = "sparklyr",
      body
    )
  }

  # accepted -> returns the host portion of amHostHttpAddress
  base_mocks(
    "ACCEPTED",
    expect_equal(spark_yarn_cluster_get_gateway(list(), 0), "amhost")
  )

  # still in a pre-accepted state -> error
  base_mocks(
    "SUBMITTED",
    expect_error(spark_yarn_cluster_get_gateway(list(), 0), "was not accepted")
  )

  # unexpected state -> error
  base_mocks(
    "RUNNING",
    expect_error(spark_yarn_cluster_get_gateway(list(), 0), "while 'ACCEPTED'")
  )

  # no resource manager webapp available -> error before any polling
  with_mocked_bindings(
    spark_yarn_cluster_get_resource_manager_webapp = function() character(),
    .package = "sparklyr",
    expect_error(
      spark_yarn_cluster_get_gateway(list(), 0),
      "not present in yarn-site.xml"
    )
  )
})

test_that("spark_connection_yarn_ui() builds a URL from config or yarn-site", {
  # explicit override wins
  sc <- list(config = list(sparklyr.web.yarn = "http://my-yarn:8088"))
  expect_equal(spark_connection_yarn_ui(sc), "http://my-yarn:8088")

  # derived from spark_web + resourcemanager.address
  sc <- list(config = list())
  with_mocked_bindings(
    spark_web = function(sc) "http://driver-host:4040/jobs",
    spark_yarn_get_conf_property = function(property, fails = TRUE) {
      if (property == "yarn.resourcemanager.address") {
        "rmhost:8032"
      } else {
        character()
      }
    },
    .package = "sparklyr",
    expect_equal(spark_connection_yarn_ui(sc), "http://rmhost:8088")
  )

  # derived using resourcemanager.hostname
  with_mocked_bindings(
    spark_web = function(sc) "http://driver-host:4040/jobs",
    spark_yarn_get_conf_property = function(property, fails = TRUE) {
      if (property == "yarn.resourcemanager.hostname") "rmhost" else character()
    },
    .package = "sparklyr",
    expect_match(spark_connection_yarn_ui(sc), "rmhost")
  )
})

# ---------------------------------------------------------------------------
# Live integration. Skipped unless a real connection is available.
# ---------------------------------------------------------------------------
skip_connection("connection_yarn")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("'spark_connection_yarn_ui()' can build a default URL", {
  expect_true(
    nchar(spark_connection_yarn_ui(sc)) > 0
  )
})

test_clear_cache()
