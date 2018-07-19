context("ml feature tokenizer")

sc <- testthat_spark_connection()

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
