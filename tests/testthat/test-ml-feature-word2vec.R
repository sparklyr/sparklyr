context("ml feature - word2vec")

sc <- testthat_spark_connection()

test_that("ft_word2vec() returns result with correct length", {
  test_requires("dplyr")
  sentence_df <- data.frame(
    sentence = c("Hi I heard about Spark",
                 "I wish Java could use case classes",
                 "Logistic regression models are neat"))
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)
  result <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_word2vec("words", "result", vector_size = 3, min_count = 0) %>%
    pull(result)

  expect_equal(sapply(result, length), c(3, 3, 3))
})
