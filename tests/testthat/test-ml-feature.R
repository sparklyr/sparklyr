context("ml feature")

sc <- testthat_spark_connection()

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

test_that("ml_hashing_tf() input checking works", {
  expect_identical(class(ml_hashing_tf(sc, "in", "out", num_features = 25)$param_map$num_features),
                   "integer")
  expect_error(ml_hashing_tf(sc, "in", "out", binary = 1),
               "length-one logical vector")
})
