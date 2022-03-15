sc <- testthat_spark_connection()

test_that("sparklyr.nested can query nested columns", {
  if (!"sparklyr.nested" %in% installed.packages()) {
    skip("sparklyr.nested not installed.")
  }

  iris_tbl <- testthat_tbl("iris")
  iris_nst <- iris_tbl %>% sparklyr.nested::sdf_nest(Species, Sepal_Width)

  expect_equal(
    iris_nst %>% filter(data$Species == "setosa") %>% count() %>% pull(n) %>% as.integer(),
    50
  )
})
