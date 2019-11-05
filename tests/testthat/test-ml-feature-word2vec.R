context("ml feature - word2vec")

test_that("ft_word2vec() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_word2vec)
})

test_that("ft_word2vec() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    vector_size = 90,
    min_count = 4,
    max_sentence_length = 1100,
    num_partitions = 2,
    step_size = 0.04,
    max_iter = 2,
    seed = 94
  )
  test_param_setting(sc, ft_word2vec, test_args)
})

test_that("ft_word2vec() returns result with correct length", {
  sc <- testthat_spark_connection()
  sentence_df <- data.frame(
    sentence = c("Hi I heard about Spark",
                 "I wish Java could use case classes",
                 "Logistic regression models are neat"))
  sentence_tbl <- sdf_copy_to(sc, sentence_df, overwrite = TRUE)
  tokenized_tbl <- ft_tokenizer(sentence_tbl, "sentence", "words") %>%
    sdf_register("tokenized")

  result <- tokenized_tbl %>%
    ft_word2vec("words", "result", vector_size = 3, min_count = 0) %>%
    pull(result)
  expect_equal(sapply(result, length), c(3, 3, 3))
})

test_that("ml_find_synonyms works properly", {
  skip_on_spark_master()
  test_requires_version("2.0.0", "spark computation different in 1.6.x")
  sc <- testthat_spark_connection()
  tokenized_tbl <- testthat_tbl("tokenized")
  model <- ft_word2vec(sc, "words", "result", vector_size= 3, min_count = 0) %>%
    ml_fit(tokenized_tbl)

  synonyms <- ml_find_synonyms(model, "java", 2) %>% pull(word)

  expect_true("models" %in% synonyms)
})

