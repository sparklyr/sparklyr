# ---- Arrow round-trip (real serialization path) ----------------------------
# Lives in a `zzz` file so it sorts LAST: attaching {arrow} puts it on the search
# path, which flips arrow_enabled() for the rest of the session (every later
# spark_apply()/collect() would take the arrow path), so we quarantine it here.
# Gated on {arrow} being installed; the live serialization path (arrow_copy_to /
# arrow_collect / arrow_read_stream) is otherwise only exercised by the dedicated
# arrow CI job. The pure arrow_enabled* predicates live in test-arrow.R.

skip_connection("arrow")
skip_on_livy()
skip_on_arrow_devel()
skip_if_not_installed("arrow")

library(arrow)

sc <- testthat_spark_connection()

test_that("copy_to + collect use the arrow serialization path", {
  expect_true(arrow_enabled(sc, sdf_len(sc, 1)))

  # arrow_copy_to, including the factor -> character branch
  df <- data.frame(
    x = 1:10,
    y = letters[1:10],
    f = factor(rep(c("a", "b"), 5)),
    stringsAsFactors = FALSE
  )
  tbl <- sdf_copy_to(sc, df, "zarrow", overwrite = TRUE)
  expect_equal(sdf_nrow(tbl), 10)

  # arrow_collect (bind_rows path) + arrow_read_stream
  back <- collect(tbl)
  expect_equal(nrow(back), 10)
  expect_setequal(back$x, 1:10)
})

test_that("arrow_collect honors n and a per-batch callback", {
  expect_equal(nrow(collect(sdf_len(sc, 20), n = 5)), 5)

  # 1-arg callback -> cb(batch)
  seen <- 0L
  sdf_collect(
    sdf_len(sc, 12),
    callback = function(batch) seen <<- seen + nrow(batch)
  )
  expect_equal(seen, 12)

  # formula callback -> coerced via rlang::as_closure + the cb(batch, iter) arm
  seen2 <- 0L
  sdf_collect(
    sdf_len(sc, 6),
    callback = ~ {
      seen2 <<- seen2 + nrow(.x)
    }
  )
  expect_equal(seen2, 6)
})

test_that("arrow_enabled_object disables arrow for vector (VectorUDT) columns", {
  v <- sdf_copy_to(
    sc,
    data.frame(a = 1:3, b = 4:6),
    "zav",
    overwrite = TRUE
  ) %>%
    ft_vector_assembler(c("a", "b"), "features")
  expect_warning(
    enabled <- arrow_enabled_object(spark_dataframe(v)),
    "Arrow disabled"
  )
  expect_false(enabled)
})

test_clear_cache()
