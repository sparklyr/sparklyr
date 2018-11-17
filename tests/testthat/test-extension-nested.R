context("extension sparklyr.nested")
sc <- testthat_spark_connection()

test_that("sparklyr.nested can query nested columns", {
  if (package_version(paste(R.Version()$major, R.Version()$minor, sep = ".")) < "3.3")
    skip("sparklyr.nested requires R 3.3")

  iris_tbl <- testthat_tbl("iris")
  iris_nst <- iris_tbl %>% sparklyr.nested::sdf_nest(Species, Sepal_Width)

  expect_equal(
    iris_nst %>% filter(data.Species == "setosa") %>% count() %>% pull(n) %>% as.integer(),
    50
  )
})
