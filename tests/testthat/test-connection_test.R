skip_connection("connection_test")
skip_on_livy()
skip_on_arrow_devel()
sc <- testthat_spark_connection()

test_that("test connection does not fail", {
  sc <- spark_connect(master = "test", method = "test")

  expect_true(!is.null(sc))
})

test_that("test_connection invoke / sdf / hive methods round-trip", {
  sc_test <- spark_connect(master = "test", method = "test")

  jobj <- sc_test$state$spark_context
  expect_equal(invoke(jobj, "version"), "1.0.0")
  expect_s3_class(invoke(jobj, "anythingElse"), "test_jobj")

  expect_s3_class(invoke_static(sc_test, "some.Class", "method"), "test_jobj")
  expect_s3_class(invoke_new(sc_test, "some.Class"), "test_jobj")
  expect_s3_class(create_hive_context(sc_test), "test_jobj")

  df <- data.frame(a = 1:3)
  expect_identical(sdf_copy_to(sc_test, df), df)
  # sdf_import dispatches on its first argument, so the test_connection method
  # is reached by passing the connection there (it simply returns it)
  expect_identical(sdf_import(sc_test, sc_test), sc_test)
})

test_clear_cache()
