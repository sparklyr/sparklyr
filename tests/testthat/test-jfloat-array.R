skip_connection("jfloat-array")
skip_on_livy()
skip_on_arrow_devel()

test_that("jfloat_array() works as expected", {
  sc <- testthat_spark_connection()
  x <- c(-1.23e-3, 0, 1.23e-3)
  jflt_arr <- jfloat_array(sc, x)

  expect_true(inherits(jflt_arr, "spark_jobj"))

  for (method in c("readJFloatArray", "readFloatArray")) {
    expect_equal(
      invoke_static(sc, "sparklyr.Test", method, jflt_arr),
      x,
      tolerance = 5e-08,
      scale = 1
    )
  }
})
