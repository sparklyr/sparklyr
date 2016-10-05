context("feature transformers")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")
austen_tbl <- testthat_tbl("austen")

test_that("ft_binarizer() works as expected", {
  skip_on_cran()

  threshold <- 3.5
  mutated <- mtcars_tbl %>%
    sdf_mutate(drat_binary = ft_binarizer(drat, threshold = threshold))

  expect_identical(
    sdf_read_column(mutated, "drat_binary"),
    as.numeric(mtcars$drat > 3.5)
  )

})

test_that("ft_bucketizer() works as expected", {
  skip_on_cran()

  splits <- c(-Inf, 2, 4, Inf)
  mutated <- mtcars_tbl %>%
    sdf_mutate(buckets = ft_bucketizer(drat, splits))

  buckets   <- sdf_read_column(mutated, "buckets")
  cutpoints <- as.numeric(cut(mtcars$drat, c(-Inf, 2, 4, Inf))) - 1

  expect_identical(buckets, cutpoints)
})

test_that("ft_tokenizer() works as expected", {
  skip_on_cran()
  skip_if_not_installed("janeaustenr")

  # NOTE: to my surprise, the ft_tokenizer does not
  # split on '\\s+', rather, just plain old '\\s'
  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    filter(length(text) > 0) %>%
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

test_that("ft_regex_tokenizer() works as expected", {
  skip_on_cran()
  skip_if_not_installed("janeaustenr")

  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    filter(length(text) > 0) %>%
    head(10) %>%
    sdf_mutate(tokens = ft_regex_tokenizer(text, pattern = "\\s+")) %>%
    sdf_read_column("tokens") %>%
    lapply(unlist)

  r_tokens <- austen %>%
    filter(nzchar(text)) %>%
    head(10) %>%
    `$`("text") %>%
    tolower() %>%
    strsplit("\\s+")

  expect_identical(spark_tokens, r_tokens)

})

test_that("the feature transforming family of functions has consistent API", {
  skip_on_cran()

  ns <- asNamespace("sparklyr")
  exports <- getNamespaceExports(ns)
  fts <- grep("^ft_", exports, value = TRUE)

  for (ft in fts) {
    transformer <- get(ft, envir = ns, mode = "function")
    fmls <- names(formals(transformer))
    expect_true(all(c("input.col", "output.col", "...") %in% fmls))
  }
})
