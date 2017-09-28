context("ml feature (transformers)")

sc <- testthat_spark_connection()

# Tokenizer

test_that("We can instantiate tokenizer object", {
  tokenizer <- ft_tokenizer(sc, "x", "y", uid = "tok")
  expect_equal(tokenizer$type, "org.apache.spark.ml.feature.Tokenizer")
  expect_equal(tokenizer$uid, "tok")
  expect_equal(class(tokenizer), c("ml_transformer", "ml_pipeline_stage"))
})


test_that("ft_tokenizer() returns params of transformer", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  expected_params <- list("x", "y")
  expect_true(dplyr::setequal(tokenizer$param_map, expected_params))
})

test_that("ft_tokenizer.tbl_spark() works as expected", {
  # skip_on_cran()
  test_requires("janeaustenr")
  austen     <- austen_books()
  austen_tbl <- testthat_tbl("austen")

  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    filter(length(text) > 0) %>%
    head(10) %>%
    ft_tokenizer("text", "tokens") %>%
    sdf_read_column("tokens") %>%
    lapply(unlist)

  r_tokens <- austen %>%
    filter(nzchar(text)) %>%
    head(10) %>%
    `$`("text") %>%
    tolower() %>%
    strsplit("\\s")

  expect_identical(spark_tokens, r_tokens)
})

# Binarizer

test_that("ft_binarizer() returns params of transformer", {
  binarizer <- ft_binarizer(sc, "x", "y", threshold = 0.5)
  params <- list("x", "y", threshold = 0.5)
  expect_true(dplyr::setequal(binarizer$param_map, params))
})

test_that("ft_binarizer.tbl_spark() works as expected", {
  test_requires("dplyr")
  df <- data.frame(id = 0:2L, feature = c(0.1, 0.8, 0.2))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  expect_equal(
    df_tbl %>%
      ft_binarizer("feature", "binarized_feature", threshold = 0.5) %>%
      collect(),
    df %>%
      mutate(binarized_feature = c(0.0, 1.0, 0.0))
  )
})

test_that("ft_binarizer() threshold defaults to 0", {
  expect_identical(ft_binarizer(sc, "in", "out")$param_map$threshold,
                   0)
})

test_that("ft_binarizer() input checking works", {
  expect_identical(class(ft_binarizer(sc, "in", "out", 1L)$param_map$threshold),
                   "numeric")
  expect_error(ft_binarizer(sc, "in", "out", "foo"),
               "length-one numeric vector")
})

# HashingTF

test_that("ft_hashing_tf() input checking works", {
  expect_identical(class(ft_hashing_tf(sc, "in", "out", num_features = 25)$param_map$num_features),
                   "integer")
  expect_error(ft_hashing_tf(sc, "in", "out", binary = 1),
               "length-one logical vector")
})

# DCT

test_that("ft_dct() works", {
  df <- data.frame(
    f1 = c(0, -1, 14),
    f2 = c(1, 2, -2),
    f3 = c(-2, 4, -5),
    f4 = c(3, -7, 1)
  )

  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)
  out1 <- df_tbl %>%
    ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
    ft_dct("features", "featuresDCT") %>%
    dplyr::pull(featuresDCT)

  # check backwards compatibility
  out2 <- df_tbl %>%
    ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
    ft_discrete_cosine_transform("features", "featuresDCT") %>%
    dplyr::pull(featuresDCT)

  expected_out <- list(
    c(1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604),
    c(-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677),
    c(4.0,9.304453421915744,11.000000000000002,1.5579302036357163)
  )

  expect_equal(out1, expected_out)
  expect_equal(out2, expected_out)
})

# IndexToString

test_that("ft_index_to_string() works", {
  df <- dplyr::data_frame(string = c("foo", "bar", "foo", "foo"))
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)

  s1 <- df_tbl %>%
    ft_string_indexer("string", "indexed") %>%
    ft_index_to_string("indexed", "string2") %>%
    dplyr::pull(string2)

  expect_identical(s1, c("foo", "bar", "foo", "foo"))

  s2 <- df_tbl %>%
    ft_string_indexer("string", "indexed") %>%
    ft_index_to_string("indexed", "string2", c("wow", "cool")) %>%
    dplyr::pull(string2)

  expect_identical(s2, c("wow", "cool", "wow", "wow"))
})

# ElementwiseProduct

test_that("ft_elementwise_product() works", {
  df <- data.frame(a = 1, b = 3, c = 5)
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)

  nums <- df_tbl %>%
    ft_vector_assembler(list("a", "b", "c"), output_col = "features") %>%
    ft_elementwise_product("features", "multiplied", c(2, 4, 6)) %>%
    dplyr::pull(multiplied) %>%
    rlang::flatten_dbl()

  expect_identical(nums,
                   c(1, 3, 5) * c(2, 4, 6))

})

# RegexTokenizer

test_that("ft_regex_tokenizer() works", {
  test_requires("dplyr")
  sentence_df <- data_frame(
    id = c(0, 1, 2),
    sentence = c("Hi I heard about Spark",
                 "I wish Java could use case classes",
                 "Logistic,regression,models,are,neat")
  )
  sentence_tbl <- testthat_tbl("sentence_df")

  expect_identical(
    sentence_tbl %>%
      ft_regex_tokenizer("sentence", "words", pattern = "\\W") %>%
      collect() %>%
      mutate(words = sapply(words, length)) %>%
      pull(words),
    c(5L, 7L, 5L))
})

# StopWordsRemover

test_that("ft_stop_words_remover() works", {
  test_requires("dplyr")
  df <- data_frame(id = c(0, 1),
                   raw = c("I saw the red balloon", "Mary had a little lamb"))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_identical(
    df_tbl %>%
      ft_tokenizer("raw", "words") %>%
      ft_stop_words_remover("words", "filtered") %>%
      pull(filtered),
    list(list("saw", "red", "balloon"), list("mary", "little", "lamb"))
  )

  expect_identical(
    df_tbl %>%
      ft_tokenizer("raw", "words") %>%
      ft_stop_words_remover("words", "filtered", stop_words = list("I", "Mary", "lamb")) %>%
      pull(filtered),
    list(list("saw", "the", "red", "balloon"), list("had", "a", "little"))
  )

  swr <- ft_stop_words_remover(
    sc, "input", "output", case_sensitive = TRUE,
    stop_words = as.list(letters), uid = "hello")

  expect_equal(
    ml_get_params(swr, list(
    "input_col", "output_col", "case_sensitive", "stop_words")),
    list(input_col = "input",
         output_col = "output",
         case_sensitive = TRUE,
         stop_words = as.list(letters))
  )
})
