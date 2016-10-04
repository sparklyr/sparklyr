context("feature transformers")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")
austen_tbl <- testthat_tbl("austen")

test_that("ft_binarizer() works as expected", {

  threshold <- 3.5
  mutated <- mtcars_tbl %>%
    sdf_mutate(drat_binary = ft_binarizer(drat, threshold = threshold))

  expect_identical(
    sdf_read_column(mutated, "drat_binary"),
    as.numeric(mtcars$drat > 3.5)
  )

})

test_that("ft_bucketizer() works as expected", {

  splits <- c(-Inf, 2, 4, Inf)
  mutated <- mtcars_tbl %>%
    sdf_mutate(buckets = ft_bucketizer(drat, splits))

  buckets   <- sdf_read_column(mutated, "buckets")
  cutpoints <- as.numeric(cut(mtcars$drat, c(-Inf, 2, 4, Inf))) - 1

  expect_identical(buckets, cutpoints)
})

test_that("ft_tokenizer() works as expected", {

  # NOTE: to my surprise, the ft_tokenizer does not
  # split on '\\s+', rather, just plain old '\\s'
  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    head(10) %>%
    sdf_mutate(tokens = ft_tokenizer(text)) %>%
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

