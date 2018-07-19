context("ml feature regex tokenizer")

sc <- testthat_spark_connection()

test_that("ft_regex_tokenizer() works", {
  test_requires("dplyr")
  sentence_df <- data_frame(
    id = c(0, 1, 2),
    sentence = c("Hi I heard about Spark",
                 "I wish Java could use case classes",
                 "Logistic,regression,models,are,neat")
  )
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)

  expect_identical(
    sentence_tbl %>%
      ft_regex_tokenizer("sentence", "words", pattern = "\\W") %>%
      collect() %>%
      mutate(words = sapply(words, length)) %>%
      pull(words),
    c(5L, 7L, 5L))

  rt <- ft_regex_tokenizer(
    sc, "sentence", "words",
    gaps = TRUE, min_token_length = 2, pattern = "\\W", to_lower_case = FALSE)

  expect_equal(
    ml_params(rt, list(
      "input_col", "output_col", "gaps", "min_token_length", "pattern", "to_lower_case"
    )),
    list(input_col = "sentence",
         output_col = "words",
         gaps = TRUE,
         min_token_length = 2L,
         pattern = "\\W",
         to_lower_case = FALSE)
  )

})
