context("dbi")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
dbi_df <- data.frame(a = 1:3, b = letters[1:3])

test_that("dbWriteTable can write a table", {
  if (spark_version(sc) < "2.2.0") skip("tables not supported before 2.0.0")

  test_requires("DBI")

  dbWriteTable(sc, "dbi_persists", dbi_df)
  dbWriteTable(sc, "dbi_temporary", dbi_df, temporary = TRUE)

  tables <- dbGetQuery(sc, "SHOW TABLES")

  expect_equal(
    tables[tables$tableName == "dbi_persists", ]$isTemporary,
    FALSE
  )

  expect_equal(
    tables[tables$tableName == "dbi_temporary", ]$isTemporary,
    TRUE
  )
})

test_that("dbGetQuery works with parameterized queries", {
  setosa <- dbGetQuery(sc, "SELECT * FROM iris WHERE species = ?", "setosa")
  expect_equal(nrow(setosa), 50)

  virginica <- dbGetQuery(sc, "SELECT * FROM iris WHERE species = ?virginica", virginica = "virginica")
  expect_equal(nrow(virginica), 50)
})
