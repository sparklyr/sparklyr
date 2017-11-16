context("ml feature - backwards compat")

test_requires("dplyr")
test_requires("janeaustenr")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")
iris_tbl   <- testthat_tbl("iris")

austen     <- austen_books()
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

test_that("ft_quantile_discretizer() works with basic input", {
  test_requires_version("2.0.0", "unidentified bug affecting <2.0.0")
  skip_on_cran()

  # Fails in 1.6.x
  # > iris_tbl %>% ft_quantile_discretizer(input.col = "Sepal_Length", output.col = "Group", n.buckets = 2) %>% group_by(Group) %>% summarize(count = n())
  # # Source:   lazy query [?? x 2]
  # # Database: spark_connection
  #    Group count
  #    <dbl> <dbl>
  # 1     0    65
  # 2     1    84
  # 3     2     1

  # https://github.com/rstudio/sparklyr/issues/341
  # previously failed due to incorrect assertion on 'n.buckets' type
  result <- iris_tbl %>%
    ft_quantile_discretizer(input.col = "Sepal_Length",
                            output.col = "Group",
                            n.buckets = 2)

  grouped <- result %>%
    group_by(Group) %>% summarize(n = n())

  expect_equal(sdf_nrow(grouped), 2)
})
