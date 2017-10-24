context("ml clustering - lda")

sc <- testthat_spark_connection()

test_that("ml_lda param setting", {
  args <- list(
    x = sc, k = 9, max_iter = 11, doc_concentration = 1.2,
    topic_concentration = 1.3, subsampling_rate = 0.04,
    optimizer = "em", checkpoint_interval = 8,
    keep_last_checkpoint = FALSE, learning_decay = 0.52,
    learning_offset = 1000, optimize_doc_concentration = FALSE,
    seed = 89, features_col = "fcol", topic_distribution_col = "tdcol"
  )
  predictor <- do.call(ml_lda, args)
  args_to_check <- if (spark_version(sc) >= "2.0.0")
    setdiff(names(args), "x")
  else
    setdiff(names(args), c("x", "keep_last_checkpoint"))
  expect_equal(ml_params(predictor, args_to_check), args[args_to_check])
})

test_that("ml_lda() default params are correct", {

  predictor <- ml_pipeline(sc) %>%
    ml_lda() %>%
    ml_stage(1)

  args <- get_default_args(
    ml_lda,
    c("x", "uid", "...", "keep_last_checkpoint", "doc_concentration", "topic_concentration", "seed"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})
