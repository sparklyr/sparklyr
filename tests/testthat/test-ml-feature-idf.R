context("ml feature - idf")

test_that("ft_idf() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_idf)
})

test_that("ft_idf() param setting", {
  test_requires_latest_spark()
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
    sentence = c("Hi I heard about Spark",
                 "I wish Java could use case classes",
                 "Logistic regression models are neat"))
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)
  idf_1 <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_hashing_tf("words", "rawFeatures", num_features = 20) %>%
    ft_idf("rawFeatures", "features") %>%
    pull(features) %>%
    first()

  expect_equal(which(idf_1 != 0), c(1, 6, 10, 18))
  expect_equal(idf_1[which(idf_1 != 0)],
               c(0.693147180559945, 0.693147180559945, 0.287682072451781, 1.38629436111989
  ), tol = 1e-4)
})
