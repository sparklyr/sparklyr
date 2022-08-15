skip_on_livy()
skip_on_arrow_devel()

test_that("spark_dependency_fallback() works correctly", {
  expect_equal(
    spark_dependency_fallback("2.3", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.3")),
    "2.1"
  )
})
