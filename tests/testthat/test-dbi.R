
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
dbi_df <- data.frame(a = 1:3, b = letters[1:3])

persisted_table_name <- random_table_name("persisted_")
temp_table_name <- random_table_name("temporary_")

test_that("dbWriteTable can write a table", {
  if (spark_version(sc) < "2.2.0") skip("tables not supported before 2.0.0")

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
  dbi_df_content <- dbGetQuery(sc, paste("SELECT * FROM ", persisted_table_name))

  expect_equal(dbi_df[c("a")], dbi_df_content)
})

test_that("dbGetQuery works with parameterized queries", {
  setosa <- dbGetQuery(sc, "SELECT * FROM iris WHERE species = ?", "setosa")
  expect_equal(nrow(setosa), 50)

  virginica <- dbGetQuery(sc, "SELECT * FROM iris WHERE species = ?virginica", virginica = "virginica")
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

  expect_equal(columns_info$name, c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species"))
  expect_equal(columns_info$type, c("numeric", "numeric", "numeric", "numeric", "character"))
  expect_equal(columns_info$sql.type, c("DoubleType", "DoubleType", "DoubleType", "DoubleType", "StringType"))
})

test_that("dbListTables list all the existing tables", {
  expect_true(persisted_table_name %in% dbListTables(sc))
})

test_that("dbIsValid detects when a connection is opened", {
  expect_true(dbIsValid(sc))
})

teardown({
  dbRemoveTable(sc, persisted_table_name)
  dbRemoveTable(sc, temp_table_name)
})
