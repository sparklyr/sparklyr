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

test_that("we can invoke_static 'package object' types", {
  expect_equal(
    invoke_static(sc, "sparklyr.test", "testPackageObject", "x"),
    "x"
  )
})

test_that("we can invoke methods with Char/Short/Long/Float parameters (#1395)", {
  expect_identical(
    invoke_new(sc, "java.lang.Character", "f"),
    "f"
  )

  expect_identical(
    invoke_new(sc, "java.lang.Short", 42L),
    42L
  )

  expect_identical(
    invoke_new(sc, "java.lang.Long", 42),
    42
  )

  expect_identical(
    invoke_new(sc, "java.lang.Long", 42L),
    42
  )

  expect_identical(
    invoke_new(sc, "java.lang.Float", 42),
    42
  )
})

test_that("numeric to Long out of range error", {
  big_number <- invoke_static(sc, "scala.Long", "MaxValue") * 2
  expect_error(
    invoke_new(sc, "java.lang.Long", big_number),
    "java\\.lang\\.Exception: Unable to cast numeric to Long: out of range\\."
  )
  expect_error(
    invoke_new(sc, "java.lang.Long", -1 * big_number),
    "java\\.lang\\.Exception: Unable to cast numeric to Long: out of range\\."
  )
})

test_that("integer to Short out of range error", {
  big_number <- invoke_static(sc, "scala.Short", "MaxValue") * 2
  expect_error(
    invoke_new(sc, "java.lang.Short", as.integer(big_number)),
    "java\\.lang\\.Exception: Unable to cast integer to Short: out of range\\."
  )
  expect_error(
    invoke_new(sc, "java.lang.Short", as.integer(-1 * big_number)),
    "java\\.lang\\.Exception: Unable to cast integer to Short: out of range\\."
  )
})
