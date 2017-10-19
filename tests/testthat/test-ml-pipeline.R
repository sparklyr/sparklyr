context("ml pipeline")

sc <- testthat_spark_connection()

training <- data_frame(
  id = 0:3L,
  text = c("a b c d e spark",
           "b d",
           "spark f g h",
           "hadoop mapreduce"),
  label = c(1, 0, 1, 0)
)

training_tbl <- testthat_tbl("training")

test <- data_frame(
  id = 4:7L,
  text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
)
test_tbl <- testthat_tbl("test")

test_that("ml_pipeline() returns a c('ml_pipeline', 'ml_estimator', 'ml_pipeline_stage')", {
  p <- ml_pipeline(sc)
  expect_equal(class(p), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
  expect_equal(ml_stages(p), NA)
  expect_equal(jobj_class(spark_jobj(p))[1], "Pipeline")
  uid_prefix <- gsub(pattern = "_.+$", replacement = "", p$uid)
  expect_equal(uid_prefix, "pipeline")
})

test_that("ml_pipeline() combines pipeline_stages into a pipeline", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)
  pipeline <- ml_pipeline(tokenizer, binarizer)
  individual_stage_uids <- c(tokenizer$uid, binarizer$uid)
  expect_equal(pipeline$stage_uids, individual_stage_uids)
  expect_equal(class(pipeline), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
})

test_that("we can create nested pipelines", {
  p0 <- ml_pipeline(sc)
  tokenizer <- ft_tokenizer(sc, "x", "y")
  pipeline <- ml_pipeline(p0, tokenizer)
  expect_equal(class(ml_stage(pipeline, 1))[1], "ml_pipeline")
  expect_equal(ml_stage(pipeline, 1) %>% ml_stages(), NA)
})

test_that("ml_transformer.ml_pipeline() works as expected", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)

  p1 <- ml_pipeline(tokenizer, binarizer)

  p2 <- ml_pipeline(sc) %>%
    ft_tokenizer("x", "y") %>%
    ft_binarizer("in", "out", 0.5)

  p1_params <- p1 %>%
    ml_stages() %>%
    lapply(ml_param_map)
  p2_params <- p2 %>%
    ml_stages() %>%
    lapply(ml_param_map)
  expect_equal(p1_params, p2_params)
  expect_equal(class(p2)[1], "ml_pipeline")
})
