context("ml feature")

sc <- testthat_spark_connection()

test_that("We can instantiate tokenizer object", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  stage_class <- tokenizer$stages[["tok", exact = FALSE]]$.stage %>%
    invoke("getClass") %>%
    invoke("getName")
  expect_equal(stage_class, "org.apache.spark.ml.feature.Tokenizer")
})

test_that("ml_tokenizer() should return a Pipeline object", {
  # we want all R objects to have a corresponding Pipeline jobj
  #   for consistency rather than worry about Pipeline vs PipelineStage.
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  pipeline_class <- tokenizer$.pipeline %>%
    invoke("getClass") %>%
    invoke("getName")
  expect_equal(class(tokenizer)[1], "ml_pipeline")
  expect_equal(pipeline_class, "org.apache.spark.ml.Pipeline")
})

test_that("ml_tokenizer() output should return type of transformer", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  stage <- tokenizer$stages$tok
  expect_equal(stage[["type"]], "org.apache.spark.ml.feature.Tokenizer")
})

test_that("ml_tokenizer() uses Spark uid for stage name", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  stage_name <- names(tokenizer$stages)
  uid <- tokenizer$stages[[stage_name]]$.stage %>%
    invoke("uid")
  expect_equal(stage_name, uid)
})

test_that("ml_tokenizer() returns params of transformer", {
  tokenizer <- ml_tokenizer(sc, input_col = "x", output_col = "y")
  params <- list(input_col = "x", output_col = "y")
  expect_true(dplyr::setequal(tokenizer$stages$tok$params, params))
})

test_that("ml_tokenizer.tbl_spark() works as expected", {
  # skip_on_cran()
  test_requires("janeaustenr")
  austen     <- austen_books()
  austen_tbl <- testthat_tbl("austen")

  spark_tokens <- austen_tbl %>%
    na.omit() %>%
    filter(length(text) > 0) %>%
    head(10) %>%
    ml_tokenizer(input_col = "text", output_col = "tokens") %>%
    sdf_read_column("tokens") %>%
    lapply(unlist)

  r_tokens <- austen %>%
    filter(nzchar(text)) %>%
    head(10) %>%
    `$`("text") %>%
    tolower() %>%
    strsplit("\\s")

  expect_identical(spark_tokens, r_tokens)
})

test_that("ml_binarizer() returns params of transformer", {
  binarizer <- ml_binarizer(sc, input_col = "x", output_col = "y", threshold = 0.5)
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
