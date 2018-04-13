context("ml stat - chisq")

sc <- testthat_spark_connection()
df_tbl <- sdf_copy_to(sc, data.frame(
  gender = sample(c("F", "M"), 200,replace = TRUE),
  party = sample(c("D", "I", "R"), 200,replace = TRUE),
  stringsAsFactors = FALSE
), overwrite = TRUE)

test_that("ml_chisquare_test() works", {
  test_requires_version("2.2.0", "chisquare test supported in spark 2.2+")
  expect_identical(
    df_tbl %>%
    ml_chisquare_test(features = "gender",
                      label = "party") %>%
    names(),
    c("feature", "label", "p_value",
      "degrees_of_freedom", "statistic")
  )
})

test_that("ml_chisquare_test() errors on bad column spec", {
  test_requires_version("2.2.0", "chisquare test supported in spark 2.2+")
  expect_error(
    df_tbl %>%
      ml_chisquare_test(features = "foo",
                        label = "bar"),
    "All columns specified must be in x\\. Failed to find foo, bar\\."
  )
})
