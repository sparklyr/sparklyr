skip_connection("core_gateway")
skip_on_livy()
skip_on_arrow_devel()

test_that("master_is_gateway recognizes gateway master URLs", {
  expect_true(master_is_gateway("sparklyr://10.0.0.1:8880"))
  expect_true(master_is_gateway("10.0.0.1:8880/3"))
  expect_false(master_is_gateway("local"))
  expect_false(master_is_gateway("yarn"))
})

test_that("spark_gateway_commands exposes the protocol command codes", {
  cmds <- spark_gateway_commands()
  expect_equal(cmds[["GetPorts"]], 0)
  expect_equal(cmds[["RegisterInstance"]], 1)
})

test_that("gateway_connection rejects a non-gateway master", {
  expect_error(
    gateway_connection("local", list()),
    "expected to be formatted as sparklyr://"
  )
})

test_that("wait_connect_gateway returns NULL when the socket never connects", {
  with_mocked_bindings(
    socketConnection = function(...) stop("connection refused"),
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    expect_null(
      wait_connect_gateway(
        "localhost", 9999,
        list(sparklyr.gateway.timeout = 0.05),
        isStarting = FALSE
      )
    )
  )
})

test_that("spark_connect_gateway returns NULL or aborts when the gateway is down", {
  with_mocked_bindings(
    wait_connect_gateway = function(...) NULL,
    .package = "sparklyr",
    {
      expect_null(
        spark_connect_gateway("localhost", 9999, 1, list(), isStarting = FALSE)
      )
      expect_error(
        spark_connect_gateway("localhost", 9999, 1, list(), isStarting = TRUE),
        "did not respond"
      )
    }
  )
})

test_that("spark_gateway_connection aborts when the backend socket fails", {
  gw <- list(gateway = textConnection("x"), backendPort = 9999)
  # the error handler closes gw$gateway; guard the fixture cleanup either way
  withr::defer(tryCatch(close(gw$gateway), error = function(e) NULL))
  with_mocked_bindings(
    socketConnection = function(...) stop("refused"),
    .package = "base",
    expect_error(
      spark_gateway_connection("sparklyr://h:8880", list(), gw, "h"),
      "Failed to open connection to backend"
    )
  )
})

test_that("query_gateway_for_port returns ports when the gateway responds", {
  # readInt feeds: backendSessionId, redirectGatewayPort, backendPort
  reads <- c(1L, 8880L, 9001L)
  i <- 0L
  with_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      writeInt = function(...) invisible(NULL),
      readInt = function(...) {
        i <<- i + 1L
        reads[[i]]
      },
      .package = "sparklyr",
      {
        res <- query_gateway_for_port(
          gateway = NULL,
          sessionId = 1,
          config = list(),
          isStarting = TRUE
        )
        expect_equal(res$redirectGatewayPort, 8880L)
        expect_equal(res$backendPort, 9001L)
      }
    )
  )
})

test_that("query_gateway_for_port returns NULL when not starting and silent", {
  with_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      writeInt = function(...) invisible(NULL),
      readInt = function(...) integer(0),
      .package = "sparklyr",
      expect_null(
        query_gateway_for_port(
          gateway = NULL,
          sessionId = 1,
          config = list(sparklyr.gateway.timeout = 0.05),
          isStarting = FALSE
        )
      )
    )
  )
})

test_that("query_gateway_for_port aborts when starting and silent", {
  with_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      writeInt = function(...) invisible(NULL),
      readInt = function(...) integer(0),
      .package = "sparklyr",
      expect_error(
        query_gateway_for_port(
          gateway = NULL,
          sessionId = 1,
          config = list(sparklyr.connect.timeout = 0.05),
          isStarting = TRUE
        ),
        "did not respond while retrieving ports"
      )
    )
  )
})

test_that("spark_connect_gateway returns the backend when the port matches", {
  with_mocked_bindings(
    wait_connect_gateway = function(...) "gw",
    query_gateway_for_port = function(...) {
      list(gateway = "gw", backendPort = 9001L, redirectGatewayPort = 8880L)
    },
    .package = "sparklyr",
    {
      res <- spark_connect_gateway("h", 8880L, 1, list(), isStarting = FALSE)
      expect_equal(res$backendPort, 9001L)
    }
  )
})

test_that("spark_connect_gateway follows a redirect to a new gateway port", {
  i <- 0L
  with_mocked_bindings(
    close = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      wait_connect_gateway = function(...) "gw",
      query_gateway_for_port = function(...) {
        i <<- i + 1L
        # first response redirects away from 8880, second response matches 9999
        list(gateway = "gw", backendPort = 9001L, redirectGatewayPort = 9999L)
      },
      .package = "sparklyr",
      {
        res <- spark_connect_gateway("h", 8880L, 1, list(), isStarting = FALSE)
        expect_equal(res$backendPort, 9001L)
      }
    )
  )
  expect_equal(i, 2L)
})

test_that("spark_connect_gateway handles an unregistered session", {
  with_mocked_bindings(
    close = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      wait_connect_gateway = function(...) "gw",
      query_gateway_for_port = function(...) {
        list(gateway = "gw", backendPort = 0L, redirectGatewayPort = 0L)
      },
      .package = "sparklyr",
      {
        expect_null(
          spark_connect_gateway("h", 8880L, 1, list(), isStarting = FALSE)
        )
        expect_error(
          spark_connect_gateway("h", 8880L, 1, list(), isStarting = TRUE),
          "does not have the requested session registered"
        )
      }
    )
  )
})

test_that("spark_connect_gateway surfaces a failing port query", {
  attempts <- 0L
  with_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      wait_connect_gateway = function(...) "gw",
      query_gateway_for_port = function(...) {
        attempts <<- attempts + 1L
        stop("ports query boom")
      },
      .package = "sparklyr",
      expect_error(
        spark_connect_gateway(
          "h",
          8880L,
          1,
          list(
            sparklyr.gateway.port.query.attempts = 2L,
            sparklyr.gateway.port.query.retry.interval.seconds = 0L
          ),
          isStarting = FALSE
        ),
        "ports query boom"
      )
    )
  )
  expect_equal(attempts, 1L)
})

test_that("gateway_connection parses the master and aborts when connect fails", {
  with_mocked_bindings(
    spark_connect_gateway = function(gatewayAddress,
                                     gatewayPort,
                                     sessionId,
                                     config,
                                     ...) {
      # the master URL is split into address / port / session id
      expect_equal(gatewayAddress, "10.0.0.5")
      expect_equal(gatewayPort, 8880L)
      expect_equal(sessionId, 7L)
      NULL
    },
    .package = "sparklyr",
    expect_error(
      gateway_connection("sparklyr://10.0.0.5:8880/7", list()),
      "Failed to connect to gateway"
    )
  )
})

test_that("gateway_connection builds a connection on success", {
  fake_sc <- structure(list(), class = "spark_gateway_connection")
  with_mocked_bindings(
    spark_connect_gateway = function(...) list(gateway = "gw", backendPort = 9001L),
    spark_gateway_connection = function(...) fake_sc,
    .package = "sparklyr",
    expect_identical(
      gateway_connection("sparklyr://h:8880", list()),
      fake_sc
    )
  )
})

test_that("spark_log / spark_web are unavailable over a gateway connection", {
  sc <- structure(
    list(),
    class = c("spark_gateway_connection", "spark_connection")
  )
  expect_error(spark_log(sc), "not available")
  expect_error(spark_web(sc), "not available")
})

test_clear_cache()
