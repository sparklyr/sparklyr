context("ml feature count vectorizer")

test_that("ft_count_vectorizer() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_count_vectorizer)
})

test_that("ft_count_vectorizer() param setting", {
  test_requires_latest_spark()
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
  df <- data_frame(text = c("a b c", "a a a b b c"))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  counts <- df_tbl %>%
    ft_tokenizer("text", "words") %>%
    ft_count_vectorizer("words", "features") %>%
    pull(features)

  counts2 <- df_tbl %>%
    ft_tokenizer("text", "words") %>%
    ft_count_vectorizer(., "words", "features", dataset = .) %>%
    pull(features)

  expect_identical(counts, list(c(1, 1, 1), c(3, 2, 1)))
  expect_identical(counts2, list(c(1, 1, 1), c(3, 2, 1)))

  # correct classes
  expect_identical(class(ft_count_vectorizer(sc, "words", "features"))[1],
                   "ml_count_vectorizer")

  cv_model <- ft_count_vectorizer(sc, "words", "features") %>%
    ml_fit(ft_tokenizer(df_tbl, "text", "words"))

  expect_identical(class(cv_model)[1],
                   "ml_count_vectorizer_model")

  # vocab extraction
  expect_identical(cv_model$vocabulary, list("a", "b", "c"))

  cv <- ft_count_vectorizer(
    sc, "words", "features", binary = TRUE, min_df = 2, min_tf = 2,
    vocab_size = 1024
  )

  expect_equal(ml_params(cv, list(
    "input_col", "output_col", "binary", "min_df", "min_tf", "vocab_size"
  )),
  list(input_col = "words",
       output_col = "features",
       binary = TRUE,
       min_df = 2L,
       min_tf = 2L,
       vocab_size = 1024L)
  )
})
