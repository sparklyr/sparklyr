context("ml feature stop words remover")

test_that("ft_stop_words_remover() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_stop_words_remover)
})

test_that("ft_stop_words_remover() param setting", {
  test_requires_latest_spark()
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
    ml_params(swr, list(
      "input_col", "output_col", "case_sensitive", "stop_words")),
    list(input_col = "input",
         output_col = "output",
         case_sensitive = TRUE,
         stop_words = as.list(letters))
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
