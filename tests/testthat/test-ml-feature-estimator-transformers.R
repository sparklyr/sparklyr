context("ml feature (estimators)")

sc <- testthat_spark_connection()

test_that("ft_count_vectorizer() works", {
  df <- data_frame(text = c("a b c", "a a a b b c"))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  counts <- df_tbl %>%
    ft_tokenizer("text", "words") %>%
    ft_count_vectorizer("words", "features") %>%
    pull(features)

  expect_identical(counts, list(c(1, 1, 1), c(3, 2, 1)))

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

  expect_equal(ml_get_params(cv, list(
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

test_that("ft_quantile_discretizer works", {
  df <- data_frame(id = 0:4L,
                   hour = c(18, 19, 8, 5, 2))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_identical(df_tbl %>%
                     ft_quantile_discretizer("hour", "result", num_buckets = 3) %>%
                     pull(result),
                   c(2, 2, 1, 1, 0)
  )


  qd <- ft_quantile_discretizer(
    sc, "hour", "result", handle_invalid = "skip",
    num_buckets = 3L, relative_error = 0.01)
  expect_equal(ml_get_params(qd, list(
    "input_col", "output_col", "handle_invalid", "num_buckets", "relative_error"
  )),
  list(input_col = "hour",
       output_col = "result",
       handle_invalid = "skip",
       num_buckets = 3L,
       relative_error = 0.01))
})
