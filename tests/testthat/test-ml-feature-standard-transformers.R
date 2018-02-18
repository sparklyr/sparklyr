context("ml feature (transformers)")

sc <- testthat_spark_connection()

# Tokenizer

test_that("We can instantiate tokenizer object", {
  tokenizer <- ft_tokenizer(sc, "x", "y", uid = "tok")
  expect_equal(jobj_class(spark_jobj(tokenizer), simple_name = FALSE)[1], "org.apache.spark.ml.feature.Tokenizer")
  expect_equal(tokenizer$uid, "tok")
  expect_equal(class(tokenizer), c("ml_tokenizer", "ml_transformer", "ml_pipeline_stage"))
})


test_that("ft_tokenizer() returns params of transformer", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  expected_params <- list("x", "y")
  expect_true(dplyr::setequal(ml_param_map(tokenizer), expected_params))
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
  expect_true(dplyr::setequal(ml_param_map(binarizer), params))
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
  expect_identical(ft_binarizer(sc, "in", "out") %>%
                     ml_param("threshold"),
                   0)
})

test_that("ft_binarizer() input checking works", {
  expect_identical(ft_binarizer(sc, "in", "out", 1L) %>%
                     ml_param("threshold") %>%
                     class(),
                   "numeric")
  expect_error(ft_binarizer(sc, "in", "out", "foo"),
               "length-one numeric vector")

  bin <- ft_binarizer(sc, "in", "out", threshold = 10)
  expect_equal(ml_params(bin, list("input_col", "output_col", "threshold")),
               list(input_col = "in", output_col = "out", threshold = 10))
})

# HashingTF

test_that("ft_hashing_tf() works", {
  args <- list(x = sc, input_col = "in", output_col = "out", num_features = 1024) %>%
    param_add_version("2.0.0", binary = TRUE)

  transformer <- do.call(ft_hashing_tf, args)

  expect_equal(ml_params(transformer, names(args)[-1]), args[-1])
})

test_that('ft_hashing_tf() input checking', {
  test_requires_version("2.0.0", "binary arg requires spark 2.0+")
  expect_error(ft_hashing_tf(sc, "in", "out", binary = 1),
               "length-one logical vector")
})

# ElementwiseProduct

test_that("ft_elementwise_product() works", {
  test_requires_version('2.0.0', "elementwise product requires spark 2.0+")
  df <- data.frame(a = 1, b = 3, c = 5)
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)

  nums <- df_tbl %>%
    ft_vector_assembler(list("a", "b", "c"), output_col = "features") %>%
    ft_elementwise_product("features", "multiplied", c(2, 4, 6)) %>%
    dplyr::pull(multiplied) %>%
    rlang::flatten_dbl()

  expect_identical(nums,
                   c(1, 3, 5) * c(2, 4, 6))

  ewp <- ft_elementwise_product(
    sc, "features", "multiplied", scaling_vec = c(1, 3, 5))

  expect_equal(
    ml_params(ewp, list(
      "input_col", "output_col", "scaling_vec"
    )),
    list(input_col = "features",
         output_col = "multiplied",
         scaling_vec = c(1, 3, 5))
  )


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
