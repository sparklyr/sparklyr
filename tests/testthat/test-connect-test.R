skip_on_livy()
skip_on_arrow_devel()
sc <- testthat_spark_connection()

test_that("test connection does not fail", {
  sc <- spark_connect(master = "test", method = "test")

  expect_true(!is.null(sc))
})
