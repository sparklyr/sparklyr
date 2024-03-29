skip_connection("ml-stat-chisq")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
sc <- testthat_spark_connection()
df_tbl <- sdf_copy_to(sc, data.frame(
  gender = sample(c("F", "M"), 200, replace = TRUE),
  party = sample(c("D", "I", "R"), 200, replace = TRUE),
  stringsAsFactors = FALSE
), overwrite = TRUE)

test_that("ml_chisquare_test() works", {
  test_requires_version("2.2.0", "chisquare test supported in spark 2.2+")

  expect_warning_on_arrow(
    m_c <- df_tbl %>%
      ml_chisquare_test(
        features = "gender",
        label = "party"
      ) %>%
      names()
  )

  expect_identical(
    m_c,
    c(
      "feature", "label", "p_value",
      "degrees_of_freedom", "statistic"
    )
  )
})

test_that("ml_chisquare_test() errors on bad column spec", {
  test_requires_version("2.2.0", "chisquare test supported in spark 2.2+")
  expect_error(
    df_tbl %>%
      ml_chisquare_test(
        features = "foo",
        label = "bar"
      ),
    "All columns specified must be in x\\. Failed to find foo, bar\\."
  )
})

test_clear_cache()

