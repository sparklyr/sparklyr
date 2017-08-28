context("ml pipeine")

sc <- testthat_spark_connection()

test_that("ml_stages() combines pipelines", {
  tokenizer <- ml_tokenizer(sc, "x", "y")
  binarizer <- ml_binarizer(sc, "in", "out", 0.5)
  pipeline <- tokenizer %>%
    ml_stages(binarizer)
  pipeline_stage_names <- names(pipeline$stages)
  individual_stage_names <- c(names(tokenizer$stages), names(binarizer$stages))
  expect_equal(pipeline_stage_names, individual_stage_names)
  expect_equal(class(pipeline), "ml_pipeline")
})

test_that("ml_transformer.ml_pipeline() works as expected", {
  tokenizer <- ml_tokenizer(sc, "x", "y")
  binarizer <- ml_binarizer(sc, "in", "out", 0.5)

  p1 <- ml_stages(tokenizer, binarizer)

  p2 <- ml_tokenizer(sc, "x", "y") %>%
    ml_binarizer("in", "out", 0.5)

  remove_uid <- function(params) {
    names(params) <- sub("_.*$", "", names(params))
    params
  }

  p1_params <- p1$stages %>%
    lapply(function(x) x$params) %>%
    remove_uid()
  p2_params <- p2$stages %>%
    lapply(function(x) x$params) %>%
    remove_uid()

  expect_equal(p1_params, p2_params)
  expect_equal(class(p2), "ml_pipeline")
})

test_that("ml_save_pipeline()/ml_load_pipeline() work for ml_pipeline", {
  p1 <- ml_tokenizer(sc, "x", "y") %>%
    ml_binarizer("in", "out", 0.5)
  path <- tempfile()
  ml_save_pipeline(p1, path)
  p2 <- ml_load_pipeline(sc, path)

  p1_uid <- p1$.pipeline %>%
    invoke("uid")
  p2_uid <- p2$.pipeline %>%
    invoke("uid")

  p1_params <- p1$stages %>%
    lapply(function(x) x$params)
  p2_params <- p2$stages %>%
    lapply(function(x) x$params)

  expect_equal(p1_uid, p2_uid)
  expect_equal(p1_params, p2_params)
})

test_that("ml_fit() returns a ml_pipeline_model", {
  training <- data_frame(
    id = 0:3L,
    text = c("a b c d e spark",
             "b d",
             "spark f g h",
             "hadoop mapreduce"),
    label = c(1, 0, 1, 0)
  )
  training_tbl <- copy_to(sc, training, overwrite = TRUE)

  tokenizer <- ml_tokenizer(sc, input_col = "text", output_col = "words")
  hashing_tf <- ml_hashing_tf(sc, input_col = "words", output_col = "features")
  lr <- ml_logistic_regression(sc, max_iter = 10, lambda = 0.001)
  pipeline <- ml_stages(tokenizer, hashing_tf, lr)

  model <- ml_fit(pipeline, training_tbl)
  expect_equal(class(model), "ml_pipeline_model")
})
