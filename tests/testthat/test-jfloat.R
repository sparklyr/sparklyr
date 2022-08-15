skip_on_livy()
skip_on_arrow_devel()

test_that("jfloat() works as expected", {
  sc <- testthat_spark_connection()
  x <- 1.23e-3
  jflt <- jfloat(sc, x)

  expect_true(inherits(jflt, "spark_jobj"))
  expect_equal(jflt %>% invoke("doubleValue"), x)
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "readFloat", jflt), x
  )
})
