context("ml feature tokenizer")

test_that("ft_tokenizer() param setting", {
  test_requires_latest_spark()
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
  austen     <- austen_books()
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
