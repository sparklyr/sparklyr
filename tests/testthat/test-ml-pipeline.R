context("ml pipeine")

sc <- testthat_spark_connection()

training <- data_frame(
  id = 0:3L,
  text = c("a b c d e spark",
           "b d",
           "spark f g h",
           "hadoop mapreduce"),
  label = c(1, 0, 1, 0)
)
training_tbl <- copy_to(sc, training, overwrite = TRUE)

test_that("ml_pipeline() returns a ml_pipeline", {
  p <- ml_pipeline(sc)
  expect_equal(class(p), "ml_pipeline")
  expect_equal(length(p$stages), 0)
  expect_equal(p$type, "org.apache.spark.ml.Pipeline")
  uid_prefix <- gsub(pattern = "_.+$", replacement = "", p$uid)
  expect_equal(uid_prefix, "pipeline")
})

test_that("ml_pipeline() combines pipeline_stages into a pipeline", {
  tokenizer <- ml_tokenizer(sc, "x", "y")
  binarizer <- ml_binarizer(sc, "in", "out", 0.5)
  pipeline <- ml_pipeline(tokenizer, binarizer)
  individual_stage_uids <- c(tokenizer$uid, binarizer$uid)
  expect_equal(pipeline$stage_uids, individual_stage_uids)
  expect_equal(class(pipeline)[1], "ml_pipeline")
})

test_that("ml_transformer.ml_pipeline() works as expected", {
  tokenizer <- ml_tokenizer(sc, "x", "y")
  binarizer <- ml_binarizer(sc, "in", "out", 0.5)

  p1 <- ml_pipeline(tokenizer, binarizer)

  p2 <- ml_pipeline(sc) %>%
    ml_tokenizer("x", "y") %>%
    ml_binarizer("in", "out", 0.5)

  p1_params <- p1$stages %>%
    lapply(function(x) x$param_map)
  p2_params <- p2$stages %>%
    lapply(function(x) x$param_map)
  expect_equal(p1_params, p2_params)
  expect_equal(class(p2)[1], "ml_pipeline")
})

test_that("ml_save_pipeline()/ml_load_pipeline() work for ml_pipeline", {
  p1 <- ml_pipeline(sc) %>%
    ml_tokenizer("x", "y") %>%
    ml_binarizer("in", "out", 0.5)
  path <- tempfile()
  ml_save_pipeline(p1, path)
  p2 <- ml_load_pipeline(sc, path)


  p1_params <- p1$stages %>%
    lapply(function(x) x$param_map)
  p2_params <- p2$stages %>%
    lapply(function(x) x$param_map)

  expect_equal(p1$uid, p2$uid)
  expect_equal(p1_params, p2_params)
})

test_that("ml_fit() returns a ml_pipeline_model", {

  tokenizer <- ml_tokenizer(sc, input_col = "text", output_col = "words")
  hashing_tf <- ml_hashing_tf(sc, input_col = "words", output_col = "features")
  lr <- ml_logistic_regression(sc, max_iter = 10, lambda = 0.001)
  pipeline <- ml_pipeline(tokenizer, hashing_tf, lr)

  model <- ml_fit(pipeline, training_tbl)
  expect_equal(class(model)[1], "ml_pipeline_model")
})

test_that("ml_[save/load]_model() work for ml_pipeline_model", {
  pipeline <- ml_pipeline(sc) %>%
    ml_tokenizer(input_col = "text", output_col = "words") %>%
    ml_hashing_tf(input_col = "words", output_col = "features") %>%
    ml_logistic_regression(max_iter = 10, lambda = 0.001)
  model1 <- ml_fit(pipeline, training_tbl)
  path <- tempfile("model")
  ml_save_model(model1, path)
  model2 <- ml_load_model(sc, path)
  expect_equal(model1$stage_uids, model2$stage_uids)

  test <- data_frame(
    id = 4:7L,
    text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
  )
  test_tbl <- copy_to(sc, test, overwrite = TRUE)

  score_test_set <- function(x, data) {
    x$.jobj %>%
      invoke("transform", spark_dataframe(data)) %>%
      sdf_register() %>%
      pull(probability)
  }
  expect_equal(score_test_set(model1, test_tbl), score_test_set(model2, test_tbl))

})
