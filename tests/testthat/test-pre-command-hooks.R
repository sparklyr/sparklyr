skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("pre-command hooks are successfully executed", {
  path <- tempfile("pre-command-hooks")

  sc %>% invoke_static("sparklyr.Shell", "getBackend") %>% invoke("testPreCommandHooks", path)
  sc %>% invoke_static("sparklyr.Shell", "getBackend")
  lines <- readLines(path)
  expect_equal(lines[1], "Running pre-command hooks")
})
