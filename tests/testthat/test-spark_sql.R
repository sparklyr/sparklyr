skip_connection("spark_sql")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("spark_db_desc / spark_sql_query_explain describe and explain", {
  desc <- spark_db_desc(sc)
  expect_match(desc, "spark connection")
  expect_match(desc, "master=")

  explained <- spark_sql_query_explain(sc, dbplyr::sql("SELECT 1"))
  expect_match(as.character(explained), "EXPLAIN")
})

test_that("spark_sql_query_save builds a CREATE OR REPLACE VIEW statement", {
  saved <- spark_sql_query_save(sc, dbplyr::sql("SELECT 1"), "v_tmp_sql", temporary = TRUE)
  expect_match(as.character(saved), "CREATE OR REPLACE")
  expect_match(as.character(saved), "VIEW")
})

test_that("apply_config sets prefixed key/value pairs on a Spark object", {
  conf <- invoke_new(sc, "org.apache.spark.SparkConf")
  out <- apply_config(conf, list(spark.x = TRUE, spark.y = "z"), "set", "")
  expect_true(inherits(out, "spark_jobj"))
  # empty params returns the object unchanged
  expect_identical(apply_config(conf, list(), "set", ""), conf)
})

test_that("spark_db_query_fields / spark_sql_query_fields read column names", {
  tbl <- sdf_copy_to(sc, data.frame(a = 1:3, b = c("x", "y", "z")), "sql_flds", overwrite = TRUE)
  flds <- spark_db_query_fields(sc, dbplyr::sql("SELECT * FROM sql_flds"))
  expect_setequal(flds, c("a", "b"))

  qf <- spark_sql_query_fields(sc, dbplyr::sql("SELECT * FROM sql_flds"))
  expect_match(as.character(qf), "a")
})

test_that("spark_db_analyze runs ANALYZE on a non-temporary table", {
  # persisted table so SHOW TABLES reports isTemporary = FALSE
  DBI::dbExecute(sc, "CREATE TABLE IF NOT EXISTS anz_tbl AS SELECT 1 AS a")
  on.exit(DBI::dbExecute(sc, "DROP TABLE IF EXISTS anz_tbl"), add = TRUE)
  expect_error(spark_db_analyze(sc, "anz_tbl"), NA)
})

test_clear_cache()
