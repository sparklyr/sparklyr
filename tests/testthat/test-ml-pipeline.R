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
