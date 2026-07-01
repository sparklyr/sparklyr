skip_connection("tidyr_pivot_utils")
skip_on_livy()
skip_on_arrow_devel()

test_that("canonicalize_spec rejects non-data-frames", {
  expect_error(
    canonicalize_spec(list(.name = "n", .value = "v")),
    "`spec` must be a data frame"
  )
})

test_that("canonicalize_spec requires .name and .value columns", {
  expect_error(
    canonicalize_spec(data.frame(x = 1)),
    "`spec` must have `.name` and `.value` columns"
  )
  # only one of the two present is still an error
  expect_error(
    canonicalize_spec(data.frame(.name = "n")),
    "`spec` must have `.name` and `.value` columns"
  )
})

test_that("canonicalize_spec moves .name and .value to the front", {
  spec <- data.frame(
    other = 1,
    .value = "v",
    .name = "n",
    stringsAsFactors = FALSE
  )
  out <- canonicalize_spec(spec)
  expect_equal(names(out), c(".name", ".value", "other"))
  expect_equal(out$other, 1)
})
