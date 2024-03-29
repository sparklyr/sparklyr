skip_connection("ml-feature-ngram")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_ngram() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_ngram)
})

test_that("ft_ngram() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    n = 3
  )
  test_param_setting(sc, ft_ngram, test_args)
})

test_that("ft_ngram() works properly", {
  sc <- testthat_spark_connection()
  sentence_df <- data.frame(sentence = "The purrrers on the bus go map map map")
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)
  bigrams <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_ngram("words", "bigrams", n = 2) %>%
    pull(bigrams) %>%
    unlist()

  expect_identical(
    bigrams,
    c(
      "the purrrers", "purrrers on", "on the", "the bus", "bus go",
      "go map", "map map", "map map"
    )
  )

  trigrams <- sentence_tbl %>%
    ft_tokenizer("sentence", "words") %>%
    ft_ngram("words", "trigrams", n = 3) %>%
    pull(trigrams) %>%
    unlist()

  expect_identical(
    trigrams,
    c(
      "the purrrers on", "purrrers on the", "on the bus", "the bus go",
      "bus go map", "go map map", "map map map"
    )
  )
})

test_clear_cache()

