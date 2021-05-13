context("jarray")

test_that("jarray() works as expected", {
  num_elems <- 1000
  sc <- testthat_spark_connection()
  arr <- jarray(
    sc,
    lapply(seq(num_elems), function(x) invoke_new(sc, "sparklyr.TestValue", x)),
    element_type = "sparklyr.TestValue"
  )

  expect_equal(
    invoke_static(sc, "sparklyr.Test", "readTestValueArray", arr),
    num_elems
  )
})
