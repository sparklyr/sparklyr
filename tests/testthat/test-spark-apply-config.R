skip_connection("spark-apply-config")
skip_on_livy()
test_requires("dplyr")
sc <- testthat_spark_connection()

test_that("'spark_apply' can pass environemnt variables from config", {
  expect_equal(
    sdf_len(sc, 1) %>%
      spark_apply(function(e) Sys.getenv("foo")) %>% collect() %>% as.character(),
    "env-test"
  )
})

test_clear_cache()

