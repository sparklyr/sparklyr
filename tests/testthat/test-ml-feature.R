context("ml feature")

sc <- testthat_spark_connection()

test_that("We can instantiate tokenizer object", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y", name = "tok")
  stage_class <- tokenizer$stages[["tok"]]$.stage %>%
    invoke("getClass") %>%
    invoke("getName")
  expect_equal(stage_class, "org.apache.spark.ml.feature.Tokenizer")
})

test_that("ml_tokenizer() should return a Pipeline object", {
  # we want all R objects to have a corresponding Pipeline jobj
  #   for consistency rather than worry about Pipeline vs PipelineStage.
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y", name = "tok")
  pipeline_class <- tokenizer$.pipeline %>%
    invoke("getClass") %>%
    invoke("getName")
  expect_equal(class(tokenizer)[1], "ml_pipeline")
  expect_equal(pipeline_class, "org.apache.spark.ml.Pipeline")
})

test_that("ml_tokenizer() output should return user-inputed name and type of transformer", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y", name = "tok")
  stage <- tokenizer$stages$tok
  expect_equal(stage[["name"]], "tok")
  expect_equal(stage[["type"]], "org.apache.spark.ml.feature.Tokenizer")
})
