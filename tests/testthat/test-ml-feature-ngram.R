context("ml feature - ngram")

sc <- testthat_spark_connection()

test_that("ft_ngram() works properly", {
  test_requires("dplyr")
  sentence_df <- data.frame(sentence = "The purrrers on the bus go map map map")
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)
  bigrams <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_ngram("words", "bigrams", n = 2) %>%
    pull(bigrams) %>%
    unlist()

  expect_identical(bigrams,
                   c("the purrrers", "purrrers on", "on the", "the bus", "bus go",
                     "go map", "map map", "map map")
                   )

  trigrams <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_ngram("words", "trigrams", n = 3) %>%
    pull(trigrams) %>%
    unlist()

  expect_identical(trigrams,
                   c("the purrrers on", "purrrers on the", "on the bus", "the bus go",
                     "bus go map", "go map map", "map map map")
  )
})
