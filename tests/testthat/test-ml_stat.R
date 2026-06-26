skip_connection("ml_stat")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

sc <- testthat_spark_connection()
df_tbl <- sdf_copy_to(
  sc,
  data.frame(
    gender = sample(c("F", "M"), 200, replace = TRUE),
    party = sample(c("D", "I", "R"), 200, replace = TRUE),
    stringsAsFactors = FALSE
  ),
  overwrite = TRUE
)

mtcars_tbl <- testthat_tbl("mtcars")

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
      "feature",
      "label",
      "p_value",
      "degrees_of_freedom",
      "statistic"
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

test_that("ml_corr() works", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  cor_mat <- mtcars_tbl %>%
    ml_corr()
  expect_equal(
    cor_mat %>%
      as.matrix() %>%
      diag(),
    rep(1, 11)
  )
})

test_that("ml_corr() works with assembled column", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  expect_equal(
    mtcars_tbl %>%
      ft_vector_assembler(colnames(mtcars_tbl), "features") %>%
      ml_corr("features") %>%
      as.matrix() %>%
      diag(),
    rep(1, 11)
  )
})

test_that("ml_corr() errors on bad column specification", {
  test_requires_version("2.2.0", "correlation supported in spark 2.2+")
  expect_error(
    ml_corr(mtcars_tbl, c("foo", "bar")),
    "All columns specified must be in x\\. Failed to find foo, bar."
  )
})

test_clear_cache()
