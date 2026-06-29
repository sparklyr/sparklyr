skip_connection("arrow")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

# NB: the arrow serialization round-trip (arrow_copy_to / arrow_collect /
# arrow_read_stream) requires {arrow} to be attached and is exercised by the
# dedicated arrow CI job + test-zzz-spark_apply.R. Here we cover the pure
# arrow_enabled* predicates, which decide whether that path is taken.

test_that("arrow_enabled_object predicates classify objects", {
  expect_true(arrow_enabled_object(1)) # default

  sdf <- spark_dataframe(testthat_tbl("iris"))
  expect_type(arrow_enabled_object(sdf), "logical") # spark_jobj (schema check)

  # a data frame with an unsupported list-of-raw column disables arrow + warns
  df <- dplyr::tibble(a = list(as.raw(1:2), as.raw(3:4)))
  expect_warning(enabled <- arrow_enabled_object(df), "Arrow disabled")
  expect_false(enabled)
})

test_that("arrow_enabled is FALSE when {arrow} is not attached", {
  # test-arrow.R sorts before test-zzz-spark_apply.R, so arrow is not on the
  # search path here -> arrow_enabled() short-circuits to FALSE
  expect_false(arrow_enabled(list(config = list()), data.frame(a = 1)))
})

test_clear_cache()
