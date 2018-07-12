context("spark apply config")
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("'spark_apply' can pass environemnt variables from config", {
  sc$config$sparklyr.apply.env.foo <- "env-test"

  expect_equal(
    sdf_len(sc, 1) %>%
      spark_apply(function(e) Sys.getenv("foo")) %>% collect() %>% as.character()
    ,
    "env-test"
  )
})
