context("ml pipeline")

sc <- testthat_spark_connection()

training <- dplyr::data_frame(
  id = 0:3L,
  text = c("a b c d e spark",
           "b d",
           "spark f g h",
           "hadoop mapreduce"),
  label = c(1, 0, 1, 0)
)

training_tbl <- testthat_tbl("training")

test <- dplyr::data_frame(
  id = 4:7L,
  text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
)
test_tbl <- testthat_tbl("test")

test_that("ml_pipeline() returns a c('ml_pipeline', 'ml_estimator', 'ml_pipeline_stage')", {
  p <- ml_pipeline(sc)
  expect_equal(class(p), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
  expect_equal(ml_stages(p), NULL)
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
  expect_equal(ml_stage(pipeline, 1) %>% ml_stages(), NULL)
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

test_that("empty pipeline has no stages", {
  expect_null(ml_pipeline(sc) %>% ml_stages())
})

test_that("pipeline printing works", {
  output <- capture.output(ml_pipeline(sc))
  expect_identical(output[1], "Pipeline (Estimator) with no stages")

  output <- capture.output(ml_pipeline(ft_binarizer(sc, "in", "out")))
  expect_identical(output[1], "Pipeline (Estimator) with 1 stage")

  output <- capture.output(ml_pipeline(ft_binarizer(sc, "in", "out"), ml_logistic_regression(sc)))
  expect_identical(output[1], "Pipeline (Estimator) with 2 stages")
  expect_identical(output[4], "  |--1 Binarizer (Transformer)")
  expect_identical(output[6], "  |     (Parameters -- Column Names)")
  expect_identical(output[9], "  |--2 LogisticRegression (Estimator)")
})

test_that("Error when specifying formula without tbl_spark for ml_ routines", {
  expect_error(
    ml_pipeline(sc) %>%
      ml_logistic_regression(Species ~ Petal_Length),
    "`formula` may only be specified when `x` is a `tbl_spark`\\."
  )

  expect_error(
    ml_logistic_regression(sc, Species ~ Petal_Length),
    "`formula` may only be specified when `x` is a `tbl_spark`\\."
  )
})
