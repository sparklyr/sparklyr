context("ml feature - word2vec")

sc <- testthat_spark_connection()

sentence_df <- data.frame(
  sentence = c("Hi I heard about Spark",
               "I wish Java could use case classes",
               "Logistic regression models are neat"))
tokenized_tbl <- copy_to(sc, sentence_df, overwrite = TRUE) %>%
  ft_tokenizer("sentence", "words")

test_that("ft_word2vec() returns result with correct length", {
  test_requires("dplyr")

  result <- tokenized_tbl %>%
    ft_word2vec("words", "result", vector_size = 3, min_count = 0) %>%
    pull(result)
  expect_equal(sapply(result, length), c(3, 3, 3))
})

test_that("ml_find_synonyms works properly", {
  test_requires_version("2.0.0", "spark computation different in 1.6.x")
  model <- ft_word2vec(sc, "words", "result", vector_size= 3, min_count = 0,
                       dataset = tokenized_tbl)
  expect_equal(
    ml_find_synonyms(model, "java", 2) %>%
      pull(word),
    c("models", "regression")
  )
})

