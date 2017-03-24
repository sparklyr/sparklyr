context("invoke")

sc <- testthat_spark_connection()

test_that("we can invoke_static with 0 arguments", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "nullary"), 0)
})

test_that("we can invoke_static with 1 scalar argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_primitive_int",
                               ensure_scalar_integer(5)), 25)

    expect_error(invoke_static(sc, "sparklyr.Test", "unary_primitive_int", NULL))

    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_Integer",
                               ensure_scalar_integer(5)), 25)

    expect_error(invoke_static(sc, "sparklyr.Test", "unary_Integer", NULL))

    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_nullable_Integer",
                               ensure_scalar_integer(5)), 25)

    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_nullable_Integer", NULL), -1)
})

test_that("we can invoke_static with 1 Seq argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_seq", list(3, 4)), 25)

    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_nullable_seq", list(3, 4)), 25)
})

test_that("we can invoke_static with null Seq argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unary_nullable_seq", NULL), -1)
})

test_that("infer correct overloaded method", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", 0), "Double")
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", "a"), "String")
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", list()), "Seq")
})
