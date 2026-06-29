skip_connection("connection_spark")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("pre-command hooks are successfully executed", {
  path <- tempfile("pre-command-hooks")

  sc %>%
    invoke_static("sparklyr.Shell", "getBackend") %>%
    invoke("testPreCommandHooks", path)
  sc %>% invoke_static("sparklyr.Shell", "getBackend")
  lines <- readLines(path)
  expect_equal(lines[1], "Running pre-command hooks")
})

test_that("spark API accessors return the connection's context objects", {
  expect_false(is.null(java_context(sc)))
  expect_false(is.null(spark_session(sc)))
  expect_false(is.null(hive_context(sc)))
  expect_identical(spark_connection(sc), sc)
  expect_null(spark_connection(42)) # default -> invisible NULL
})

test_that("spark_log default errors; print.spark_log prints", {
  expect_error(spark_log(42), "Invalid class")
  expect_equal(
    length(capture.output(print(structure(c("a", "b"), class = "spark_log")))),
    2
  )
})

test_that("spark_web resolves from config and from the live SparkContext", {
  sc_cfg <- sc
  sc_cfg$state <- NULL
  sc_cfg$config[["sparklyr.sparkui.url"]] <- "http://example:4040"
  expect_match(as.character(spark_web(sc_cfg)), "/jobs/")
  expect_match(as.character(spark_web(sc)), "jobs")
})

test_that("print.spark_web_url browses the url", {
  with_mocked_bindings(
    browse_url = function(x) invisible(x),
    .package = "sparklyr",
    expect_error(
      print(structure("http://x/jobs/", class = "spark_web_url")),
      NA
    )
  )
})

test_that("get_spark_sql_catalog_implementation reads the catalog property", {
  expect_type(get_spark_sql_catalog_implementation(sc), "character")
})

test_that("connection constructors build the expected classes", {
  expect_s3_class(
    new_spark_gateway_connection(list(master = "m")),
    "spark_gateway_connection"
  )
  expect_s3_class(new_livy_connection(list(master = "m")), "livy_connection")
})

test_clear_cache()
