context("ml feature chisq selector")

test_that("ft_chisq_selector() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_chisq_selector)
})

test_that("ft_chisq_selector() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    features_col = "feat",
    output_col = "foobar",
    label_col = "rawr",
    selector_type = "fpr",
    fdr = 0.1,
    fpr = 0.2,
    fwe = 0.3,
    num_top_features = 20,
    percentile = 0.15
  )
  test_param_setting(sc, ft_chisq_selector, test_args)
})

test_that("ft_chisq_select() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("2.1.0", "Spark behavior changed (https://issues.apache.org/jira/browse/SPARK-17870)")
  df <- tribble(
    ~id, ~V1, ~V2, ~V3, ~V4, ~clicked,
    7,   0,   0,   18,  1,   1,
    8,   0,   1,   12,  0,   0,
    9,   1,   0,   15,  0.1, 0
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:4), "features") %>%
    select(-dplyr::starts_with("V"))

  result <- ft_chisq_selector(
    df_tbl, "features", "selectedFeatures", "clicked",
    num_top_features = 1
  )
  expect_equal(pull(result, selectedFeatures), list(18, 12, 15))
})
