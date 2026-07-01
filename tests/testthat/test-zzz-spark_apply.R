# ---------------------------------------------------------------------------
# Arrow addendum for spark_apply(). Kept in a separate `zzz` file so it sorts
# LAST: attaching {arrow} puts it on the search path, which flips
# arrow_enabled() (it checks `"package:arrow" %in% search()`) for the rest of
# the session -- so any later test file's spark_apply()/collect() would silently
# switch to the arrow path. Running last quarantines that.
#
# Gated on arrow being *installed* (not on using_arrow()/the CI arrow flag), so
# it exercises the arrow branches of spark_apply() wherever {arrow} is present.
# ---------------------------------------------------------------------------

skip_connection("spark_apply")
skip_on_livy()
skip_on_arrow_devel()
skip_if_not_installed("arrow")

library(arrow)

sc <- testthat_spark_connection()
iris_tbl <- testthat_tbl("iris")

test_that("spark_apply() takes the arrow path for a plain apply", {
  # with {arrow} attached, arrow_enabled() is TRUE, so this exercises the
  # arrow setup block (sessionLocalTimeZone + records_per_batch) and the
  # maxRecordsPerBatch option.
  expect_true(arrow_enabled(sc, sdf_len(sc, 1)))

  result <- sdf_len(sc, 10) %>%
    spark_apply(function(df) df * 2L) %>%
    collect()

  expect_equal(nrow(result), 10)
  expect_equal(sum(result$id), sum(seq_len(10) * 2L))
})

test_that("spark_apply() takes the arrow groupBy path", {
  # group_by + arrow exercises ApplyUtils.groupByArrow.
  result <- spark_apply(
    iris_tbl,
    function(e) nrow(e),
    names = "n",
    group_by = "Species"
  ) %>%
    collect()

  expect_equal(nrow(result), 3) # one row per species
  expect_equal(sum(result$n), 150) # all rows accounted for
})

test_that("spark_apply() disables arrow when fetch_result_as_sdf = FALSE", {
  # arrow is incompatible with fetch_result_as_sdf = FALSE, so this warns and
  # falls back to the non-arrow path, returning a list of R objects.
  expect_warning(
    res <- sdf_len(sc, 3) %>%
      spark_apply(function(df) df, fetch_result_as_sdf = FALSE),
    "Disabling arrow"
  )
  expect_type(res, "list")
})

test_clear_cache()
