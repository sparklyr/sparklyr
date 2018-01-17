context("ml feature chisq selector")

sc <- testthat_spark_connection()

test_that("ft_chisq_select() works properly", {
  test_requires_version("2.1.0", "Spark behavior changed (https://issues.apache.org/jira/browse/SPARK-17870)")
  df <- dplyr::tribble(
    ~id, ~V1, ~V2, ~V3, ~V4, ~clicked,
    7,   0,   0,   18,  1,   1,
    8,   0,   1,   12,  0,   0,
    9,   1,   0,   15,  0.1, 0
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:4), "features") %>%
    dplyr::select(-dplyr::starts_with("V"))

  result <- ft_chisq_selector(
    df_tbl, "features", "selectedFeatures", "clicked",
    num_top_features = 1, dataset = df_tbl
  )
  expect_equal(dplyr::pull(result, selectedFeatures), list(18, 12, 15))
})
