skip_connection("ml_feature_text_vectorizers")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("ft_count_vectorizer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_count_vectorizer)
})

test_that("ft_count_vectorizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    binary = TRUE,
    min_df = 2,
    min_tf = 2,
    vocab_size = 2^10
  )
  test_param_setting(sc, ft_count_vectorizer, test_args)
})

test_that("ft_count_vectorizer() works", {
  test_requires_version("2.0.0", "features require Spark 2.0+")
  sc <- testthat_spark_connection()
  df <- tibble(text = c("a b c", "a a a b b c"))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    counts <- df_tbl %>%
      ft_tokenizer("text", "words") %>%
      ft_count_vectorizer("words", "features") %>%
      pull(features)
  )

  expect_warning_on_arrow(
    counts2 <- df_tbl %>%
      ft_tokenizer("text", "words") %>%
      ft_count_vectorizer("words", "features") %>%
      pull(features)
  )

  expect_identical(counts, list(c(1, 1, 1), c(3, 2, 1)))
  expect_identical(counts2, list(c(1, 1, 1), c(3, 2, 1)))

  # correct classes
  expect_identical(
    class(ft_count_vectorizer(sc, "words", "features"))[1],
    "ml_count_vectorizer"
  )

  cv_model <- ft_count_vectorizer(sc, "words", "features") %>%
    ml_fit(ft_tokenizer(df_tbl, "text", "words"))

  expect_identical(
    class(cv_model)[1],
    "ml_count_vectorizer_model"
  )

  # vocab extraction
  expect_identical(cv_model$vocabulary, list("a", "b", "c"))

  cv <- ft_count_vectorizer(
    sc,
    "words",
    "features",
    binary = TRUE,
    min_df = 2,
    min_tf = 2,
    vocab_size = 1024
  )

  expect_equal(
    ml_params(
      cv,
      list(
        "input_col",
        "output_col",
        "binary",
        "min_df",
        "min_tf",
        "vocab_size"
      )
    ),
    list(
      input_col = "words",
      output_col = "features",
      binary = TRUE,
      min_df = 2L,
      min_tf = 2L,
      vocab_size = 1024L
    )
  )
})

test_that("ft_hashing_tf() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_hashing_tf)
})

test_that("ft_hashing_tf() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    binary = TRUE,
    num_features = 2^10
  )
  test_param_setting(sc, ft_hashing_tf, test_args)
})

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
  test_requires_version(
    "2.0.0",
    "hashing implementation changed in 2.0 -- https://issues.apache.org/jira/browse/SPARK-10574"
  )
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
    list(c(
      0.287682072451781,
      0.693147180559945,
      0.287682072451781,
      0.575364144903562
    )),
    list(c(
      0.693147180559945,
      0.693147180559945,
      0.287682072451781,
      1.38629436111989
    ))
  )

  expect_equal(which(idf_1 != 0), expected_non_zero_idxes[[1]])
  expect_equal(idf_1[which(idf_1 != 0)], expected_res[[1]], tol = 1e-4)
})

test_that("ft_word2vec() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_word2vec)
})

test_that("ft_word2vec() param setting", {
  test_requires_version("3.0.0")
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
    sentence = c(
      "Hi I heard about Spark",
      "I wish Java could use case classes",
      "Logistic regression models are neat"
    )
  )
  sentence_tbl <- sdf_copy_to(sc, sentence_df, overwrite = TRUE)
  tokenized_tbl <- ft_tokenizer(sentence_tbl, "sentence", "words") %>%
    sdf_register("tokenized")

  expect_warning_on_arrow(
    result <- tokenized_tbl %>%
      ft_word2vec("words", "result", vector_size = 3, min_count = 0) %>%
      pull(result)
  )

  expect_equal(sapply(result, length), c(3, 3, 3))
})

test_that("ml_find_synonyms works properly", {
  # NOTE: this test case is functionally identical to the one in
  # https://github.com/apache/spark/blob/87b93d32a6bfb0f2127019b97b3fc1d13e16a10b/mllib/src/test/scala/org/apache/spark/mllib/feature/Word2VecSuite.scala#L37
  test_requires_version("2.0.0", "spark computation different in 1.6.x")
  sc <- testthat_spark_connection()
  sentence <- data.frame(
    sentence = do.call(paste, as.list(c(rep("a b", 100), rep("a c", 10))))
  )
  doc <- rbind(sentence, sentence)
  sdf <- sdf_copy_to(sc, doc, overwrite = TRUE)
  tokenized_tbl <- ft_tokenizer(sdf, "sentence", "words")

  model <- ft_word2vec(
    sc,
    "words",
    "result",
    vector_size = 10,
    seed = 42L,
    min_count = 0
  ) %>%
    ml_fit(tokenized_tbl)

  synonyms <- ml_find_synonyms(model, "a", 2) %>% pull(word)

  # synonym-wise "b" should be closer to "a" than "c" is
  expect_equal(synonyms, c("b", "c"))
})

test_clear_cache()
