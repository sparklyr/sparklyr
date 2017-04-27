context("ensure")

test_that("ensure_* functions work as expected", {
  expect_equal(
    "foo",
    ensure_scalar_character("foo")
  )

  expect_equal(
    NULL,
    ensure_scalar_character(NULL, allow.null = TRUE)
  )

  expect_error(
    ensure_scalar_character(NULL),
    "'NULL' is not a length-one character vector"
  )
})
