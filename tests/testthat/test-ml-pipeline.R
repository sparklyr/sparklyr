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
