skip_connection("tbl_spark")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("dim / type_sum / tbl_quote_name behave as expected", {
  tbl <- testthat_tbl("iris")
  d <- dim(tbl)
  expect_length(d, 2)
  expect_equal(d[[2]], 5L)
  expect_type(type_sum(spark_dataframe(tbl)), "character")

  expect_equal(tbl_quote_name(sc, "a.b"), "`a`.`b`")
  sc_ns <- sc
  sc_ns$config[["sparklyr.dplyr.period.splits"]] <- FALSE
  expect_match(as.character(tbl_quote_name(sc_ns, "a.b")), "a.b", fixed = TRUE)
})

test_that("tbl_cache / tbl_uncache / tbl_change_db / src_databases", {
  copy_to(sc, data.frame(x = 1:3), "tc_tbl", overwrite = TRUE)
  expect_null(tbl_cache(sc, "tc_tbl", force = TRUE))
  expect_null(tbl_uncache(sc, "tc_tbl"))
  expect_null(tbl_change_db(sc, "default"))
  # SHOW DATABASES exposes the 'namespace' column on Spark 3.x
  expect_true("default" %in% src_databases(sc, "namespace"))
})

test_clear_cache()
