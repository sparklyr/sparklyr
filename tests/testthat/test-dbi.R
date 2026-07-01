skip_connection("dbi")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
dbi_df <- data.frame(a = 1:3, b = letters[1:3])

persisted_table_name <- random_table_name("persisted_")
temp_table_name <- random_table_name("temporary_")

test_that("dbWriteTable can write a table", {
  if (spark_version(sc) < "2.2.0") {
    skip("Tables not supported before 2.0.0")
  }
  if (spark_version(sc) >= "3.3.0") {
    skip("Show Tables with Delta does not works")
  }

  test_requires("DBI")

  dbWriteTable(sc, persisted_table_name, dbi_df)
  dbWriteTable(sc, temp_table_name, dbi_df, temporary = TRUE)

  tables <- dbGetQuery(sc, "SHOW TABLES")

  expect_equal(
    tables[tables$tableName == persisted_table_name, ]$isTemporary,
    FALSE
  )

  expect_equal(
    tables[tables$tableName == temp_table_name, ]$isTemporary,
    TRUE
  )

  dbWriteTable(sc, persisted_table_name, dbi_df[c("a")], overwrite = TRUE)
  dbi_df_content <- dbGetQuery(
    sc,
    paste("SELECT * FROM ", persisted_table_name)
  )

  expect_equal(dbi_df[c("a")], dbi_df_content)
})

test_that("dbGetQuery works with parameterized queries", {
  setosa <- dbGetQuery(sc, "SELECT * FROM iris WHERE species = ?", "setosa")
  expect_equal(nrow(setosa), 50)

  virginica <- dbGetQuery(
    sc,
    "SELECT * FROM iris WHERE species = ?virginica",
    virginica = "virginica"
  )
  expect_equal(nrow(virginica), 50)
})

test_that("dbGetQuery works with native parameterized queries", {
  #TODO: databricks - Restore when we figure out how to run parametrized queries
  skip("Temp skip")
  test_requires_version("3.4.0")
  setosa <- dbGetQuery(
    conn = sc,
    statement = "SELECT * FROM iris WHERE species = :species",
    params = list(species = "setosa")
  )
  expect_equal(nrow(setosa), 50)

  virginica <- dbGetQuery(
    conn = sc,

    statement = "SELECT * FROM iris WHERE species = :virginica",
    params = list(virginica = "virginica")
  )
  expect_equal(nrow(virginica), 50)
})

test_that("dbExistsTable performs case-insensitive comparisons on table names", {
  sdf <- copy_to(sc, data.frame(a = 1, b = 2), name = "testTempView")

  expect_true(dbExistsTable(sc, "testTempView"))
  expect_true(dbExistsTable(sc, "TESTTEMPVIEW"))
  expect_true(dbExistsTable(sc, "testtempview"))
})

test_that("dbColumnInfo list the columns with its type and sql type", {
  res <- dbSendQuery(sc, "SELECT * FROM iris", "setosa")
  columns_info <- dbColumnInfo(res)

  expect_equal(
    columns_info$name,
    c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species")
  )
  expect_equal(
    columns_info$type,
    c("numeric", "numeric", "numeric", "numeric", "character")
  )
  expect_equal(
    columns_info$sql.type,
    c("DoubleType", "DoubleType", "DoubleType", "DoubleType", "StringType")
  )
})

test_that("dbListTables list all the existing tables", {
  if (spark_version(sc) >= "3.3.0") {
    skip("Show Tables with Delta does not works")
  }
  expect_true(persisted_table_name %in% dbListTables(sc))
})

test_that("dbIsValid detects when a connection is opened", {
  expect_true(dbIsValid(sc))
})

# ---- Connection-free helpers ----------------------------------------------

test_that("get_data_type maps R vectors to SQL types", {
  expect_equal(get_data_type(factor("a")), "TEXT")
  expect_equal(get_data_type(1L), "INTEGER")
  expect_equal(get_data_type(1.5), "REAL")
  expect_equal(get_data_type("x"), "STRING")
  expect_equal(get_data_type(TRUE), "INTEGER")
  expect_equal(get_data_type(list(1)), "BLOB")
  expect_error(get_data_type(1 + 2i), "Unsupported type")
})

test_that("dbi_ensure_no_backtick rejects back ticks", {
  expect_silent(dbi_ensure_no_backtick("good_name"))
  expect_error(dbi_ensure_no_backtick("bad`name"), "back tick")
})

# ---- Quoting / type methods -----------------------------------------------

test_that("dbQuoteString and dbQuoteLiteral return quoted SQL", {
  quoted <- dbQuoteString(sc, c("a", "b"))
  expect_s4_class(quoted, "SQL")
  expect_match(as.character(quoted)[[1]], '"a"')

  expect_s4_class(dbQuoteLiteral(sc, "x"), "SQL")
})

test_that("dbQuoteIdentifier backticks plain names and passes through the rest", {
  expect_match(as.character(dbQuoteIdentifier(sc, "col")), "`col`")

  # an SQL object is returned untouched
  already <- SQL("`x`")
  expect_identical(dbQuoteIdentifier(sc, already), already)

  # a zero-length vector takes the pass-through branch
  expect_identical(dbQuoteIdentifier(sc, character(0)), character(0))

  # an already-backticked, schema-qualified name is passed through
  expect_identical(dbQuoteIdentifier(sc, "`a`.`b`"), "`a`.`b`")

  # a plain name containing a back tick errors
  expect_error(dbQuoteIdentifier(sc, "a`b"), "back tick")
})

test_that("dbDataType dispatches to get_data_type", {
  expect_equal(dbDataType(sc, 1L), "INTEGER")
  expect_equal(dbDataType(sc, "x"), "STRING")
})

test_that("sqlInterpolate delegates to the DBIConnection method", {
  out <- DBI::sqlInterpolate(
    sc,
    "SELECT * FROM t WHERE x = ?val",
    val = "a"
  )
  expect_match(as.character(out), "a")
})

# ---- Connection-level methods ---------------------------------------------

test_that("dbGetInfo, show and dbplyr_edition report connection details", {
  expect_identical(dbGetInfo(sc), sc)
  expect_output(show(sc), "<spark_connection>")
  expect_type(dbplyr_edition(sc), "integer")
})

test_that("transaction methods are no-ops returning TRUE", {
  expect_true(dbBegin(sc))
  expect_true(dbCommit(sc))
  expect_true(dbRollback(sc))
})

test_that("dbSetProperty issues a SET statement", {
  expect_error(
    dbSetProperty(sc, "spark.sql.shuffle.partitions", "10"),
    NA
  )
})

# ---- Result + fetch methods -----------------------------------------------

test_that("result metadata methods report on a query", {
  res <- dbSendQuery(sc, "SELECT * FROM iris")
  on.exit(dbClearResult(res))

  expect_equal(dbGetStatement(res), "SELECT * FROM iris")
  expect_true(dbIsValid(res))
  expect_equal(dbGetRowCount(res), 150)
  expect_equal(dbGetRowsAffected(res), 150)
  expect_true(dbHasCompleted(res))
  expect_true(dbBind(res, list()))
})

test_that("dbFetch pages through results using the stored offset", {
  res <- dbSendQuery(sc, "SELECT * FROM iris")
  on.exit(dbClearResult(res))

  first <- dbFetch(res, n = 10)
  expect_equal(nrow(first), 10)

  # a second fetch advances past the previously returned rows
  second <- dbFetch(res, n = 5)
  expect_equal(nrow(second), 5)
})

test_that("dbExecute and dbSendStatement run a statement", {
  expect_equal(dbExecute(sc, "SELECT * FROM iris"), 150)

  rs <- dbSendStatement(sc, "SELECT * FROM iris")
  expect_s4_class(rs, "DBISparkResult")
  dbClearResult(rs)
})

# ---- Table read / write / remove ------------------------------------------

test_that("dbWriteTable round-trips a temporary table and honors flags", {
  tbl_name <- random_table_name("dbi_tmp_")
  other_name <- random_table_name("dbi_tmp_")
  on.exit({
    dbRemoveTable(sc, tbl_name, fail_if_missing = FALSE)
    dbRemoveTable(sc, other_name, fail_if_missing = FALSE)
  })

  dbWriteTable(sc, tbl_name, dbi_df, temporary = TRUE)
  expect_true(dbExistsTable(sc, tbl_name))
  expect_setequal(dbReadTable(sc, tbl_name)$b, letters[1:3])

  # writing again without overwrite is rejected
  expect_error(
    dbWriteTable(sc, tbl_name, dbi_df, temporary = TRUE),
    "already exists"
  )

  # overwrite replaces the contents
  dbWriteTable(
    sc,
    tbl_name,
    dbi_df[c("a")],
    temporary = TRUE,
    overwrite = TRUE
  )
  expect_equal(colnames(dbReadTable(sc, tbl_name)), "a")

  # append to a fresh table exercises the append mode branch
  dbWriteTable(sc, other_name, dbi_df, temporary = TRUE, append = TRUE)
  expect_true(dbExistsTable(sc, other_name))

  # append and overwrite are mutually exclusive
  expect_error(
    dbWriteTable(
      sc,
      other_name,
      dbi_df,
      temporary = TRUE,
      append = TRUE,
      overwrite = TRUE
    ),
    "cannot both be"
  )
})

test_that("dbWriteTable can persist a managed table", {
  name <- random_table_name("dbi_persist_")
  on.exit(dbRemoveTable(sc, name, fail_if_missing = FALSE))

  # temporary = FALSE (the default) exercises the spark_write_table branch
  dbWriteTable(sc, name, dbi_df)
  expect_true(dbExistsTable(sc, name))
  expect_equal(nrow(dbReadTable(sc, name)), 3)

  # dropping an existing table uses the fail-if-missing (no IF EXISTS) path
  dbRemoveTable(sc, name)
  expect_false(dbExistsTable(sc, name))
})

test_that("dbListTables filters temporary helper tables and sorts", {
  name <- random_table_name("dbi_list_")
  on.exit(dbRemoveTable(sc, name, fail_if_missing = FALSE))

  dbWriteTable(sc, name, dbi_df, temporary = TRUE)
  tables <- dbListTables(sc)
  expect_true(name %in% tables)
  expect_false(any(grepl("^sparklyr_tmp_", tables)))
})

test_that("dbListTables can target a specific database", {
  db <- random_table_name("dbi_db_")
  on.exit(dbExecute(sc, paste0("DROP DATABASE IF EXISTS ", db, " CASCADE")))
  dbExecute(sc, paste0("CREATE DATABASE IF NOT EXISTS ", db))

  # exercises the `database` argument ("SHOW TABLES FROM <db>")
  expect_type(dbListTables(sc, database = db), "character")
})

test_that("dbRemoveTable validates names and tolerates missing tables", {
  expect_error(dbRemoveTable(sc, "bad`name"), "back tick")

  # fail_if_missing = FALSE adds IF EXISTS and is a no-op when absent
  expect_true(
    dbRemoveTable(sc, random_table_name("absent_"), fail_if_missing = FALSE)
  )
})

teardown({
  # These tables are only created by tests that are skipped on newer Spark
  # versions, so tolerate them being absent during cleanup.
  dbRemoveTable(sc, persisted_table_name, fail_if_missing = FALSE)
  dbRemoveTable(sc, temp_table_name, fail_if_missing = FALSE)
})

test_clear_cache()
