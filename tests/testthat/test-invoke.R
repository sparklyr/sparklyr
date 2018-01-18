context("invoke")

sc <- testthat_spark_connection()

test_that("we can invoke_static with 0 arguments", {
  expect_equal(invoke_static(sc, "sparklyr.Test", "nullary"), 0)
})

test_that("we can invoke_static with 1 scalar argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryPrimitiveInt",
                               ensure_scalar_integer(5)), 25)

    expect_error(invoke_static(sc, "sparklyr.Test", "unaryPrimitiveInt", NULL))

    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryInteger",
                               ensure_scalar_integer(5)), 25)

    expect_error(invoke_static(sc, "sparklyr.Test", "unaryInteger", NULL))

    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryNullableInteger",
                               ensure_scalar_integer(5)), 25)

    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryNullableInteger", NULL), -1)
})

test_that("we can invoke_static with 1 Seq argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unarySeq", list(3, 4)), 25)

    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryNullableSeq", list(3, 4)), 25)
})

test_that("we can invoke_static with null Seq argument", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "unaryNullableSeq", NULL), -1)
})

test_that("infer correct overloaded method", {
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", 0), "Double")
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", "a"), "String")
    expect_equal(invoke_static(sc, "sparklyr.Test", "infer", list()), "Seq")
})

test_that("roundtrip date array", {
  dates <- list(as.Date("2016/1/1"), as.Date("2016/1/1"))
  expect_equal(
    invoke_static(sc, "sparklyr.Test", "roundtrip", dates),
    do.call("c", dates)
  )
})

test_that("we can invoke_static using make_ensure_scalar_impl", {
  test_ensure_scalar_integer <- make_ensure_scalar_impl(
    is.numeric,
    "a length-one integer vector",
    as.integer
  )

  expect_equal(invoke_static(sc, "sparklyr.Test", "unaryPrimitiveInt",
                             test_ensure_scalar_integer(5)), 25)
})
