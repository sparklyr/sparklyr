skip_connection("ml-feature-idf")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_idf() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_idf)
})

test_that("ft_idf() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    min_doc_freq = 2
  )
  test_param_setting(sc, ft_idf, test_args)
})

test_that("ft_idf() works properly", {
  test_requires_version("2.0.0", "hashing implementation changed in 2.0 -- https://issues.apache.org/jira/browse/SPARK-10574")
  sc <- testthat_spark_connection()
  sentence_df <- data.frame(
    sentence = c(
      "Hi I heard about Spark",
      "I wish Java could use case classes",
      "Logistic regression models are neat"
    )
  )
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)

  expect_warning_on_arrow(
    idf_1 <- sentence_tbl %>%
      ft_tokenizer("sentence", "words") %>%
      ft_hashing_tf("words", "rawFeatures", num_features = 20) %>%
      ft_idf("rawFeatures", "features") %>%
      pull(features) %>%
      first()
  )

  # hashing implementation changed in 3.0 -- https://issues.apache.org/jira/browse/SPARK-23469
  expected_non_zero_idxes <- ifelse(
    spark_version(sc) >= "3.0.0",
    list(c(7, 9, 14, 17)),
    list(c(1, 6, 10, 18))
  )
  expected_res <- ifelse(
    spark_version(sc) >= "3.0.0",
    list(c(0.287682072451781, 0.693147180559945, 0.287682072451781, 0.575364144903562)),
    list(c(0.693147180559945, 0.693147180559945, 0.287682072451781, 1.38629436111989))
  )

  expect_equal(which(idf_1 != 0), expected_non_zero_idxes[[1]])
  expect_equal(idf_1[which(idf_1 != 0)], expected_res[[1]], tol = 1e-4)
})

test_clear_cache()

