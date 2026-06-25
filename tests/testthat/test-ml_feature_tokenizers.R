skip_connection("ml_feature_tokenizers")
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
      "the purrrers",
      "purrrers on",
      "on the",
      "the bus",
      "bus go",
      "go map",
      "map map",
      "map map"
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
      "the purrrers on",
      "purrrers on the",
      "on the bus",
      "the bus go",
      "bus go map",
      "go map map",
      "map map map"
    )
  )
})

test_that("ft_regex_tokenizer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_regex_tokenizer)
})

test_that("ft_regex_tokenizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    gaps = FALSE,
    min_token_length = 2,
    pattern = "foo",
    to_lower_case = FALSE
  )
  test_param_setting(sc, ft_regex_tokenizer, test_args)
})

test_that("ft_regex_tokenizer() works", {
  sc <- testthat_spark_connection()
  sentence_df <- tibble(
    id = c(0, 1, 2),
    sentence = c(
      "Hi I heard about Spark",
      "I wish Java could use case classes",
      "Logistic,regression,models,are,neat"
    )
  )
  sentence_tbl <- copy_to(sc, sentence_df, overwrite = TRUE)

  expect_identical(
    sentence_tbl %>%
      ft_regex_tokenizer("sentence", "words", pattern = "\\W") %>%
      collect() %>%
      mutate(words = sapply(words, length)) %>%
      pull(words),
    c(5L, 7L, 5L)
  )

  rt <- ft_regex_tokenizer(
    sc,
    "sentence",
    "words",
    gaps = TRUE,
    min_token_length = 2,
    pattern = "\\W",
    to_lower_case = FALSE
  )

  expect_equal(
    ml_params(
      rt,
      list(
        "input_col",
        "output_col",
        "gaps",
        "min_token_length",
        "pattern",
        "to_lower_case"
      )
    ),
    list(
      input_col = "sentence",
      output_col = "words",
      gaps = TRUE,
      min_token_length = 2L,
      pattern = "\\W",
      to_lower_case = FALSE
    )
  )
})

test_that("ft_stop_words_remover() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_stop_words_remover)
})

test_that("ft_stop_words_remover() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    case_sensitive = TRUE,
    stop_words = c("foo", "bar")
  )
  test_param_setting(sc, ft_stop_words_remover, test_args)
})


test_that("ft_stop_words_remover() works", {
  test_requires_version("2.0.0", "loadDefaultStopWords requires Spark 2.0+")
  sc <- testthat_spark_connection()
  df <- tibble(
    id = c(0, 1),
    raw = c("I saw the red balloon", "Mary had a little lamb")
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_identical(
    df_tbl %>%
      ft_tokenizer("raw", "words") %>%
      ft_stop_words_remover("words", "filtered") %>%
      pull(filtered) %>%
      lapply(as.list),
    list(list("saw", "red", "balloon"), list("mary", "little", "lamb"))
  )

  expect_identical(
    df_tbl %>%
      ft_tokenizer("raw", "words") %>%
      ft_stop_words_remover(
        "words",
        "filtered",
        stop_words = list("I", "Mary", "lamb")
      ) %>%
      pull(filtered) %>%
      lapply(as.list),
    list(list("saw", "the", "red", "balloon"), list("had", "a", "little"))
  )

  swr <- ft_stop_words_remover(
    sc,
    "input",
    "output",
    case_sensitive = TRUE,
    stop_words = as.list(letters),
    uid = "hello"
  )

  expect_equal(
    ml_params(
      swr,
      list(
        "input_col",
        "output_col",
        "case_sensitive",
        "stop_words"
      )
    ),
    list(
      input_col = "input",
      output_col = "output",
      case_sensitive = TRUE,
      stop_words = as.list(letters)
    )
  )
})

test_that("ml_default_stop_words() defaults to English (#1280)", {
  test_requires_version("2.0.0", "loadDefaultStopWords requires Spark 2.0+")
  sc <- testthat_spark_connection()
  expect_identical(
    ml_default_stop_words(sc) %>%
      head(5),
    list("i", "me", "my", "myself", "we")
  )
})

test_that("ft_tokenizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar"
  )
  test_param_setting(sc, ft_tokenizer, test_args)
})

test_that("ft_tokenizer.tbl_spark() works as expected", {
  sc <- testthat_spark_connection()
  test_requires("janeaustenr")
  austen <- austen_books()
  austen_tbl <- testthat_tbl("austen")

  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    dplyr::filter(length(text) > 0) %>%
    head(10) %>%
    ft_tokenizer("text", "tokens") %>%
    sdf_read_column("tokens") %>%
    lapply(unlist)

  r_tokens <- austen %>%
    dplyr::filter(nzchar(text)) %>%
    head(10) %>%
    `$`("text") %>%
    tolower() %>%
    strsplit("\\s")

  expect_identical(spark_tokens, r_tokens)
})

test_clear_cache()
