context("read-write")

sc <- testthat_spark_connection()

test_that("spark_read_csv() succeeds when column contains similar non-ascii", {
  csv <- file("test.csv", "w+", encoding = "latin1")
  cat("Município;var;var 1.0\n1233;1;2", file=csv)
  close(csv)

  df <- spark_read_csv(sc,name="teste",path="test.csv",header = TRUE,
                       delimiter = ";",charset = "Latin1",memory = FALSE)

  expect_true(
    all(dplyr::tbl_vars(df) == c("Municipio", "var", "var_1_0")),
    info = "success reading non-ascii similar columns from csv")
})
