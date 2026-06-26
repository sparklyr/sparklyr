# ---------------------------------------------------------------------------
# Local, Livy-free tests. These exercise the pure helpers and (via mocked httr)
# the request/response layer without ever contacting a live Livy server. The
# genuinely live integration test is gated below by skip_unless_livy().
# ---------------------------------------------------------------------------

test_that("livy_config() works with extended parameters", {
  config <- livy_config(num_executors = 1)
  expect_equal(as.integer(config$livy.numExecutors), 1)
})

test_that("livy_config() works with authentication", {
  config_basic <- livy_config(username = "foo", password = "bar")
  expect_equal(
    config_basic$sparklyr.livy.auth,
    httr::authenticate("foo", "bar", type = "basic")
  )

  config_negotiate <- livy_config(negotiate = TRUE)
  expect_equal(
    config_negotiate$sparklyr.livy.auth,
    httr::authenticate("", "", type = "gssnegotiate")
  )
})

test_that("livy_config() works with additional_curl_opts", {
  config <- livy_config()
  expect_null(config$sparklyr.livy.curl_opts)

  curl_opts <- list(accepttimeout_ms = 5000, verbose = 1)
  config <- livy_config(curl_opts = curl_opts)
  expect_equal(config$sparklyr.livy.curl_opts, curl_opts)
})

test_that("livy_config() sets proxy, default headers, and rejects bad params", {
  config <- livy_config(proxy = httr::use_proxy("localhost", 9999))
  expect_equal(config$sparklyr.livy.proxy, httr::use_proxy("localhost", 9999))

  # default custom header
  expect_equal(config$sparklyr.livy.headers, list("X-Requested-By" = "sparklyr"))

  expect_error(
    livy_config(not_a_real_param = 1),
    "are not valid session parameters"
  )
})

test_that("livy_get_httr_config() merges headers, proxy, and curl opts", {
  config <- livy_config(
    proxy = httr::use_proxy("localhost", 9999),
    curl_opts = list(verbose = 1)
  )
  httr_config <- livy_get_httr_config(config, list("Content-Type" = "application/json"))
  expect_true(!is.null(httr_config$options))
})

test_that("livy_config_get() filters spark.* but not spark.sql.*", {
  config <- list("spark.foo" = "bar", "spark.sql.shuffle" = "200")
  out <- livy_config_get("local", config)
  expect_true("spark.foo" %in% names(out))
  expect_false("spark.sql.shuffle" %in% names(out))
})

test_that("livy_config_get_prefix() returns NULL when nothing matches", {
  expect_null(livy_config_get_prefix("local", list(), "spark.", c("spark.sql.")))
})

test_that("livy_available_jars() returns version strings", {
  jars <- livy_available_jars()
  expect_type(jars, "character")
  expect_true(length(jars) > 0)
})

test_that("livy_serialized_chunks() splits a string into n-sized chunks", {
  expect_equal(livy_serialized_chunks("abcdefghij", 3), c("abc", "def", "ghi", "j"))
  expect_equal(livy_serialized_chunks("abc", 10), "abc")
})

test_that("livy_statement_new() and livy_jobj_create() build expected structures", {
  st <- livy_statement_new("code", list(varName = "x"))
  expect_equal(st$code, "code")

  jobj <- livy_jobj_create(sc = "sc", varName = "v")
  expect_s3_class(jobj, "livy_jobj")
  expect_equal(jobj$varName, "v")
})

test_that("livy_code_new_return_var() names and increments return vars", {
  sc <- list(code = new.env())
  sc$code$totalReturnVars <- 0
  expect_equal(livy_code_new_return_var(sc), "sparklyrRetVar_0")
  expect_equal(livy_code_new_return_var(sc), "sparklyrRetVar_1")
  expect_equal(sc$code$totalReturnVars, 2)
})

test_that("livy_statement_compose_magic() builds a magic statement", {
  st <- livy_statement_compose_magic(list(varName = "v"), "json")
  expect_equal(st$code, "%json v")
  expect_null(st$lobj)
})

test_that("livy_invoke_statement_command() renders each call form", {
  expect_equal(
    livy_invoke_statement_command(NULL, TRUE, "MyClass", "<init>", FALSE),
    "// invoke_new(sc, 'MyClass', ...)"
  )
  expect_equal(
    livy_invoke_statement_command(NULL, TRUE, "MyClass", "doThing", TRUE),
    "// j_invoke_static(sc, 'MyClass', 'doThing', ...)"
  )
  expect_equal(
    livy_invoke_statement_command(NULL, FALSE, list(), "doThing", FALSE),
    "// invoke(sc, <jobj>, 'doThing', ...)"
  )
})

test_that("livy_inspect() returns NULL", {
  expect_null(livy_inspect(list()))
})

test_that("livy_log_operation() appends truncated text to the log file", {
  sc <- list(log = tempfile(fileext = ".log"))
  livy_log_operation(sc, "hello world")
  expect_equal(readLines(sc$log), "hello world")
})

test_that("livy_connection_not_used_warn() warns only on non-default values", {
  expect_warning(livy_connection_not_used_warn("custom", "default", "thing"))
  expect_silent(livy_connection_not_used_warn("default", "default", "thing"))
})

test_that("livy_states_info() reports connected status per state", {
  states <- livy_states_info()
  expect_false(states$dead$connected)
  expect_true(states$idle$connected)
})

test_that("livy_connection_jars() picks a jar url or honors an explicit jar", {
  jars <- livy_connection_jars(list(), "3.4", NULL)
  expect_match(jars, "github.com/sparklyr")

  explicit <- livy_connection_jars(
    list(sparklyr.livy.jar = "my.jar"),
    "3.4",
    NULL
  )
  expect_equal(explicit, list("my.jar"))
})

test_that("spark_log/spark_web are unsupported for livy connections", {
  expect_error(spark_log.livy_connection(NULL), "Unsupported operation")
  expect_error(spark_web.livy_connection(NULL), "Unsupported operation")
})

test_that("livy_validate_http_response() handles success, 401, and other errors", {
  # success: no error -> returns invisibly without stopping
  with_mocked_bindings(
    http_error = function(...) FALSE,
    .package = "sparklyr",
    expect_silent(livy_validate_http_response("msg", "req"))
  )

  # 401 -> unauthorized message
  with_mocked_bindings(
    http_error = function(...) TRUE,
    status_code = function(...) 401L,
    .package = "sparklyr",
    expect_error(livy_validate_http_response("msg", "req"), "unauthorized")
  )

  # other error -> message + status + content
  with_mocked_bindings(
    http_error = function(...) TRUE,
    status_code = function(...) 500L,
    http_status = function(...) list(message = "Server Error"),
    content = function(...) "boom",
    .package = "sparklyr",
    expect_error(
      livy_validate_http_response("Failed", "req"),
      "Failed.*Server Error.*boom"
    )
  )
})

test_that("livy_get_json() issues a GET and returns parsed content", {
  with_mocked_bindings(
    GET = function(...) "req",
    http_error = function(...) FALSE,
    content = function(...) list(foo = "bar"),
    .package = "sparklyr",
    expect_equal(livy_get_json("http://host/sessions", list()), list(foo = "bar"))
  )
})

test_that("livy_get_sessions/get_session/get_statement validate their payloads", {
  with_mocked_bindings(
    livy_get_json = function(...) list(sessions = list(), total = 0),
    .package = "sparklyr",
    expect_equal(livy_get_sessions("http://host", list())$total, 0)
  )

  sc <- list(master = "http://host", sessionId = 7, config = list())
  with_mocked_bindings(
    livy_get_json = function(...) list(id = 7, state = "idle"),
    .package = "sparklyr",
    expect_equal(livy_get_session(sc)$state, "idle")
  )

  with_mocked_bindings(
    livy_get_json = function(...) list(id = 3, state = "available"),
    .package = "sparklyr",
    expect_equal(livy_get_statement(sc, 3)$state, "available")
  )
})

test_that("livy_create_session() posts and validates the session payload", {
  with_mocked_bindings(
    POST = function(...) "req",
    http_error = function(...) FALSE,
    content = function(...) list(id = 1, state = "idle", kind = "spark"),
    .package = "sparklyr",
    {
      session <- livy_create_session("http://host", list())
      expect_equal(session$id, 1)
      expect_equal(session$kind, "spark")
    }
  )
})

test_that("livy_destroy_session() deletes and confirms the session", {
  sc <- list(master = "http://host", sessionId = 1, config = list())
  with_mocked_bindings(
    DELETE = function(...) "req",
    http_error = function(...) FALSE,
    content = function(...) list(msg = "deleted"),
    .package = "sparklyr",
    expect_null(livy_destroy_session(sc))
  )
})

test_that("livy_post_statement() polls until a statement is available", {
  sc <- list(
    master = "http://host",
    sessionId = 1,
    config = list(),
    log = tempfile(fileext = ".log")
  )
  with_mocked_bindings(
    POST = function(...) "req",
    http_error = function(...) FALSE,
    content = function(...) {
      list(
        id = 1,
        state = "available",
        output = list(status = "ok", data = list("text/plain" = "result"))
      )
    },
    .package = "sparklyr",
    {
      data <- livy_post_statement(sc, "1 + 1")
      expect_equal(data[["text/plain"]], "result")
    }
  )
})

test_that("livy_invoke_statement() converts supported data types", {
  sc <- list()
  with_mocked_bindings(
    livy_post_statement = function(sc, code) list("application/json" = list(a = 1)),
    .package = "sparklyr",
    expect_equal(livy_invoke_statement(sc, list(code = "x"))$a, 1)
  )

  with_mocked_bindings(
    livy_post_statement = function(sc, code) list("unsupported/type" = "x"),
    .package = "sparklyr",
    expect_error(livy_invoke_statement(sc, list(code = "x")), "unsupported")
  )
})

test_that("livy_statement_compose() composes single- and multi-chunk statements", {
  sc <- list(code = new.env())
  sc$code$totalReturnVars <- 0

  with_mocked_bindings(
    livy_invoke_serialize = function(...) "short",
    .package = "sparklyr",
    {
      st <- livy_statement_compose(sc, TRUE, "MyClass", "doThing")
      expect_match(st$code, "invokeFromBase64")
      expect_s3_class(st$lobj, "livy_jobj")
    }
  )

  with_mocked_bindings(
    livy_invoke_serialize = function(...) paste(rep("x", 25000), collapse = ""),
    .package = "sparklyr",
    {
      st <- livy_statement_compose(sc, TRUE, "MyClass", "doThing")
      expect_match(st$code, "StringBuilder")
    }
  )
})

test_that("livy_validate_master() succeeds when sessions are reachable", {
  with_mocked_bindings(
    livy_get_sessions = function(...) list(sessions = list(), total = 0),
    .package = "sparklyr",
    expect_null(livy_validate_master("http://host", list()))
  )
})

# ---------------------------------------------------------------------------
# Live Livy integration. Skipped unless a Livy server is available. We are not
# investing further in live Livy coverage (legacy technology).
# ---------------------------------------------------------------------------
skip_connection("connection_livy")
skip_unless_livy()
skip_on_arrow_devel()

num_open_fds <- function(port) {
  n <- system2(
    "bash",
    args = c("-c", paste0("'lsof -t -i:", as.integer(port), " | wc -l'")),
    stdout = TRUE
  )

  as.integer(n)
}

test_that("Livy connection works with HTTP proxy", {
  sc <- testthat_spark_connection()
  proxy_port <- 9999
  np <- num_open_fds(proxy_port)

  if (np > 0) {
    handle <- local_tcp_proxy(proxy_port = proxy_port, dest_port = 8998)

    expect_equal(np, 1)

    config <- livy_config(proxy = httr::use_proxy("localhost", proxy_port))

    version <- testthat_spark_env_version()

    sc <- spark_connect(
      "http://localhost",
      method = "livy",
      config = config,
      version = version
    )
    expect_gte(num_open_fds(proxy_port), 2)

    expect_equivalent(
      sdf_len(sc, 10) %>% collect(),
      dplyr::tibble(id = seq(10))
    )

    spark_disconnect(sc)
  }
})

test_clear_cache()
