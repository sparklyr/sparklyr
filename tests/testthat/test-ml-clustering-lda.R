context("ml clustering - lda")

test_that("ml_lda() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_lda)
})

test_that("ml_lda() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    k = 20,
    max_iter = 40,
    doc_concentration = 1.2,
    topic_concentration = 1.4,
    subsampling_rate = 0.10,
    optimizer = "em",
    checkpoint_interval = 20,
    keep_last_checkpoint = FALSE,
    learning_decay = 0.58,
    learning_offset = 2046,
    optimize_doc_concentration = FALSE,
    seed = 234234,
    features_col = "wwaefa",
    topic_distribution_col = "eifjaewif"
  )
  test_param_setting(sc, ml_lda, test_args)
})

test_that("ml_lda() works properly", {
  sc <- testthat_spark_connection()
  sample_data_path <- dir(getwd(), recursive = TRUE, pattern = "sample_lda_libsvm_data.txt", full.names = TRUE)
  sample_data <- spark_read_libsvm(sc, "sample_data",
                                   sample_data_path, overwrite = TRUE)
  lda <- ml_lda(sample_data, k = 10, max_iter = 10, seed = 5432)
  ll <- ml_log_likelihood(lda, sample_data)
  expect_equal(ll, -805.0026, tolerance = 0.1)
  lp <- ml_log_perplexity(lda, sample_data)
  expect_equal(lp, 3.096164, tolerance = 0.1)
  topics <- ml_describe_topics(lda, 3)
  expect_identical(
    topics %>% dplyr::pull(termWeights) %>% head(1) %>% unlist() %>% length(),
    3L
  )
  expect_identical(
    colnames(ml_transform(lda, sample_data)),
    c("label", "features", "topicDistribution")
  )
  expect_identical(
    sdf_nrow(ml_transform(lda, sample_data)),
    12
  )
})

test_that("ml_lda/ft_count_vectorizer helper functions (#1353)", {
  sc <- testthat_spark_connection()
  fake_data <- data.frame(a = c(1, 2, 3, 4),
                          b = c("the groggy", "frog was", "a very groggy", "frog"))
  fake_tbl <- sdf_copy_to(sc, fake_data, overwrite = TRUE)

  fake_tokenized <- fake_tbl %>%
    ft_tokenizer(input_col = 'b', output_col = 'tokens')

  count_vectorizer_model <- ft_count_vectorizer(sc, input_col = "tokens", output_col = "features",
                                                dataset = fake_tokenized)

  fake_model <- count_vectorizer_model %>%
    ml_transform(fake_tokenized) %>%
    ml_lda(features_col = 'features', k = 2)

  topics_matrix <- ml_topics_matrix(fake_model)
  expect_identical(
    dim(topics_matrix),
    c(6L, 2L)
  )
  expect_identical(
    ml_vocabulary(count_vectorizer_model),
    unlist(count_vectorizer_model$vocabulary)
  )
})
