context("dbi")
sc <- testthat_spark_connection()

dbi_df <- data_frame(a = 1:3, b = letters[1:3])

test_that("dbWriteTable can write a table", {
  test_requires("dbi")

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
