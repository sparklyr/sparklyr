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

test_that("ml_tokenizer() uses Spark uid of transformer by default for name", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  default_name <- names(tokenizer$stages)
  uid <- tokenizer$stages[[default_name]]$.stage %>%
    invoke("uid")
  expect_equal(default_name, uid)
})

test_that("ml_tokenizer() returns params of transformer", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y", "tok")
  params <- list(input_col = "x", output_col = "y")
  expect_true(dplyr::setequal(tokenizer$stages$tok$params, params))
})

test_that("ml_binarizer() returns params of transformer", {
  binarizer <- ml_binarizer(sc, input_col = "x", output_col = "y", threshold = 0.5, name = "bin")
  params <- list(input_col = "x", output_col = "y", threshold = 0.5)
  expect_true(dplyr::setequal(binarizer$stages$bin$params, params))
})

test_that("ml_binarizer.tbl_spark() works as expected", {
  df <- data.frame(id = 0:2L, feature = c(0.1, 0.8, 0.2))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  expect_equal(
    df_tbl %>%
      ml_binarizer(input_col = "feature", output_col = "binarized_feature",
                   threshold = 0.5) %>%
      collect(),
    df %>%
      mutate(binarized_feature = c(0.0, 1.0, 0.0))
  )
})
