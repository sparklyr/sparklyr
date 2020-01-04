context("barrier")

test_that("barrier-spark_apply works", {
  test_requires_version("2.4.0")
  sc <- testthat_spark_connection()

  address <- sdf_len(sc, 1, repartition = 1) %>%
    spark_apply(~ .y$address, barrier = TRUE, columns = c(address = "character")) %>%
    collect()

  expect_true(grepl("[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}|localhost",
                    address$address[1], perl = TRUE))
})
